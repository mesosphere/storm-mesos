package storm.mesos;

import backtype.storm.scheduler.INimbus;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;
import backtype.storm.utils.LocalState;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Semaphore;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.log4j.Logger;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.CommandInfo.URI;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Protos.Value.Range;
import org.apache.mesos.Protos.Value.Ranges;
import org.apache.mesos.Protos.Value.Scalar;
import org.apache.mesos.Protos.Value.Type;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.json.simple.JSONValue;

public class MesosNimbus implements INimbus {
    public static final String CONF_EXECUTOR_URI = "mesos.executor.uri";
    public static final String CONF_MASTER_URL = "mesos.master.url";
    public static final String CONF_MASTER_FAILOVER_TIMEOUT_SECS = "mesos.master.failover.timeout.secs";
    public static final String CONF_MESOS_ALLOWED_HOSTS = "mesos.allowed.hosts";
    public static final String CONF_MESOS_DISALLOWED_HOSTS = "mesos.disallowed.hosts";
    public static final String CONF_MESOS_ROLE = "mesos.framework.role";
    public static final String CONF_MESOS_CHECKPOINT = "mesos.framework.checkpoint";
    public static final String CONF_MESOS_FRAMEWORK_NAME = "mesos.framework.name";

    public static final Logger LOG = Logger.getLogger(MesosNimbus.class);

    private static final String FRAMEWORK_ID = "FRAMEWORK_ID";
    private final Object OFFERS_LOCK = new Object();
    private RotatingMap<OfferID, Offer> _offers;

    LocalState _state;
    NimbusScheduler _scheduler;
    volatile SchedulerDriver _driver;
    Timer _timer = new Timer();

    private LocalFileServer _httpServer;
    private java.net.URI _configUrl;

    @Override
    public IScheduler getForcedScheduler() {
        return null;
    }

    @Override
    public String getHostName(Map<String, SupervisorDetails> map, String nodeId) {
        return nodeId;
    }

    public class NimbusScheduler implements Scheduler {

        Semaphore _initter;

        public NimbusScheduler(Semaphore initter) {
            _initter = initter;
        }

        @Override
        public void registered(final SchedulerDriver driver, FrameworkID id, MasterInfo masterInfo) {
            _driver = driver;
            try {
                _state.put(FRAMEWORK_ID, id.getValue());
            } catch (IOException e) {
                LOG.error("Halting process...", e);
                Runtime.getRuntime().halt(1);
            }
            _offers = new RotatingMap<OfferID, Offer>(new
                    RotatingMap.ExpiredCallback<OfferID, Offer>() {
                        @Override
                        public void expire(OfferID key, Offer val) {
                            driver.declineOffer(val.getId());
                        }
                    });
            _timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    try {
                        synchronized(OFFERS_LOCK) {
                            _offers.rotate();
                        }
                    } catch(Throwable t) {
                        LOG.error("Received fatal error Halting process...", t);
                        Runtime.getRuntime().halt(2);
                    }
                }
            }, 0, 5000);
            _initter.release();
        }

        @Override
        public void reregistered(SchedulerDriver sd, MasterInfo info) {
        }

        @Override
        public void disconnected(SchedulerDriver driver) {
        }

        @Override
        public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
            synchronized(OFFERS_LOCK) {
                _offers.clear();
                for(Offer offer: offers) {
                    if(_offers != null && isHostAccepted(offer.getHostname())) {
                        _offers.put(offer.getId(), offer);
                    } else {
                        driver.declineOffer(offer.getId());
                    }
                }
            }
        }

        @Override
        public void offerRescinded(SchedulerDriver driver, OfferID id) {
            synchronized(OFFERS_LOCK) {
                _offers.remove(id);
            }
        }

        @Override
        public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
            LOG.info("Received status update: " + status.toString());
        }

        @Override
        public void frameworkMessage(SchedulerDriver driver, ExecutorID executorId, SlaveID slaveId, byte[] data) {
        }

        @Override
        public void slaveLost(SchedulerDriver driver, SlaveID id) {
            LOG.info("Lost slave: " + id.toString());
        }

        @Override
        public void error(SchedulerDriver driver, String msg) {
            LOG.error("Received fatal error \nmsg:" + msg + "\nHalting process...");
            try {
                _httpServer.shutdown();
            } catch (Exception e) {
                // Swallow. Nothing we can do about it now.
            }
            Runtime.getRuntime().halt(2);
        }

        @Override
        public void executorLost(SchedulerDriver driver, ExecutorID executor, SlaveID slave, int status) {
        }
    }

    Map _conf;
    Set<String> _allowedHosts;
    Set<String> _disallowedHosts;

    private static Set listIntoSet(List l) {
        if(l == null) { return null; }
        else return new HashSet(l);
    }

    @Override
    public void prepare(Map conf, String localDir) {
        try {
            _conf = conf;
            _state = new LocalState(localDir);
            String id = (String) _state.get(FRAMEWORK_ID);

            _allowedHosts = listIntoSet((List)_conf.get(CONF_MESOS_ALLOWED_HOSTS));
            _disallowedHosts = listIntoSet((List)_conf.get(CONF_MESOS_DISALLOWED_HOSTS));

            Semaphore initter = new Semaphore(0);
            _scheduler = new NimbusScheduler(initter);
            Number failoverTimeout = (Number) conf.get(CONF_MASTER_FAILOVER_TIMEOUT_SECS);
            if(failoverTimeout==null) failoverTimeout = 3600;

            String role = (String) conf.get(CONF_MESOS_ROLE);
            if (role == null) role = new String("*");
            Boolean checkpoint = (Boolean) conf.get(CONF_MESOS_CHECKPOINT);
            if (checkpoint == null) checkpoint = new Boolean(false);
            String framework_name = (String) conf.get(CONF_MESOS_FRAMEWORK_NAME);
            if (framework_name == null) framework_name = new String("Storm!!!");

            FrameworkInfo.Builder finfo = FrameworkInfo.newBuilder()
                .setName(framework_name)
                .setFailoverTimeout(failoverTimeout.doubleValue())
                .setUser("")
                .setRole(role)
                .setCheckpoint(checkpoint);
            if(id!=null) {
                finfo.setId(FrameworkID.newBuilder().setValue(id).build());
            }

            _httpServer = new LocalFileServer();
            _configUrl = _httpServer.serveDir("/conf","conf");
            LOG.info("Started serving config dir under " + _configUrl);

            MesosSchedulerDriver driver =
                new MesosSchedulerDriver(
                        _scheduler,
                        finfo.build(),
                        (String) conf.get(CONF_MASTER_URL));

            driver.start();
            LOG.info("Waiting for scheduler to initialize...");
            initter.acquire();
            LOG.info("Scheduler initialized...");

        } catch(IOException e) {
            throw new RuntimeException(e);
        } catch(InterruptedException e) {
            throw new RuntimeException(e);
        } catch(Exception e) {
          throw new RuntimeException(e);
        }
    }

    private static class OfferResources {
        int cpuSlots = 0;
        int memSlots = 0;
        List<Integer> ports = new ArrayList<Integer>();

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }
    }

    private OfferResources getResources(Offer offer, double cpu, double mem) {
        OfferResources resources = new OfferResources();

        double offerCpu = 0;
        double offerMem = 0;

        for(Resource r: offer.getResourcesList()) {
            if (r.getName().equals("cpus") && r.getScalar().getValue() > offerCpu) {
                offerCpu = r.getScalar().getValue();
                resources.cpuSlots = (int) Math.floor(offerCpu / cpu);
            } else if (r.getName().equals("mem") && r.getScalar().getValue() > offerMem) {
                offerMem = r.getScalar().getValue();
                resources.memSlots = (int) Math.floor(offerMem / mem);
            }
        }

        int maxPorts = Math.min(resources.cpuSlots, resources.memSlots);

        for(Resource r: offer.getResourcesList()) {
            if(r.getName().equals("ports")) {
                for(Range range: r.getRanges().getRangeList()) {
                    if(resources.ports.size() >= maxPorts) {
                        break;
                    } else {
                        int start = (int) range.getBegin();
                        int end = (int) range.getEnd();
                        for (int p=start; p<=end; p++) {
                            resources.ports.add(p);
                            if (resources.ports.size()>=maxPorts) {
                                break;
                            }
                        }
                    }
                }
            }
        }

        LOG.debug("Offer: " + offer.toString());
        LOG.debug("Extracted resources: " + resources.toString());
        return resources;
    }

    private List<WorkerSlot> toSlots(Offer offer, double cpu, double mem) {
        OfferResources resources = getResources(offer, cpu, mem);

        List<WorkerSlot> ret = new ArrayList<WorkerSlot>();
        int availableSlots = Math.min(resources.cpuSlots, resources.memSlots);
        availableSlots = Math.min(availableSlots, resources.ports.size());
        for(int i=0; i<availableSlots; i++) {
            ret.add(new WorkerSlot(offer.getHostname(), resources.ports.get(i)));
        }
        return ret;
    }


    Map<String, Long> _firstTopologyTime = new HashMap<String, Long>();

    public boolean isHostAccepted(String hostname) {
        if(_allowedHosts != null && !_allowedHosts.contains(hostname)) return false;
        if(_disallowedHosts != null && _disallowedHosts.contains(hostname)) return false;
        return true;
    }

    @Override
    public Collection<WorkerSlot> allSlotsAvailableForScheduling(
			Collection<SupervisorDetails> existingSupervisors, Topologies topologies, Set<String> topologiesMissingAssignments) {
        synchronized(OFFERS_LOCK) {
            LOG.info("Currently have " + _offers.size() + " offers buffered");
            if (!topologiesMissingAssignments.isEmpty()) {
                LOG.info("Topologies that need assignments: " + topologiesMissingAssignments.toString());
            } else {
                LOG.info("Declining offers because no topologies need assignments");
                _offers.clear();
            }
        }

        Double cpu = null;
        Double mem = null;
        // TODO: maybe this isn't the best approach. if a topology raises #cpus keeps failing,
        // it will mess up scheduling on this cluster permanently
        for(String id: topologiesMissingAssignments) {
            TopologyDetails details = topologies.getById(id);
            double tcpu = MesosCommon.topologyCpu(_conf, details);
            double tmem = MesosCommon.topologyMem(_conf, details);
            if(cpu==null || tcpu > cpu) {
                cpu = tcpu;
            }
            if(mem==null || tmem > mem) {
                mem = tmem;
            }
        }

        // need access to how many slots are currently used to limit number of slots taken up

        List<WorkerSlot> allSlots = new ArrayList();

        if(cpu!=null && mem!=null) {
            synchronized(OFFERS_LOCK) {
                for(Offer offer: _offers.values()) {
                    allSlots.addAll(toSlots(offer, cpu, mem));
                }
            }
        }


        LOG.info("Number of available slots: " + allSlots.size());
        return allSlots;
    }

    private OfferID findOffer(WorkerSlot worker) {
        int port = worker.getPort();
        ArrayList<Offer> offers = new ArrayList(_offers.values());
        for(Offer offer: offers) {
            if(offer.getHostname().equals(worker.getNodeId())) {
                for(Resource r: offer.getResourcesList()) {
                    if(r.getName().equals("ports")) {
                        for(Range range: r.getRanges().getRangeList()) {
                            if(port >= range.getBegin() && port <= range.getEnd()) {
                                return offer.getId();
                            }
                        }
                    }
                }
            }
        }
        return null;
    }

    @Override
    public void assignSlots(Topologies topologies, Map<String, Collection<WorkerSlot>> slots) {
        synchronized(OFFERS_LOCK) {
            Map<OfferID, List<TaskInfo>> toLaunch = new HashMap();
            for(String topologyId: slots.keySet()) {
                for(WorkerSlot slot: slots.get(topologyId)) {
                    OfferID id = findOffer(slot);
                    Offer offer = _offers.get(id);
                    if(id!=null) {
                        if(!toLaunch.containsKey(id)) {
                            toLaunch.put(id, new ArrayList());
                        }
                        TopologyDetails details = topologies.getById(topologyId);
                        double cpu = MesosCommon.topologyCpu(_conf, details);
                        double mem = MesosCommon.topologyMem(_conf, details);

                        Map executorData = new HashMap();
                        executorData.put(MesosCommon.SUPERVISOR_ID, slot.getNodeId() + "-" + details.getId());
                        executorData.put(MesosCommon.ASSIGNMENT_ID, slot.getNodeId());

						// Determine roles for cpu, mem, ports
						String cpuRole = new String("*");
						String memRole = cpuRole;
						String portsRole = cpuRole;
						for(Resource r: offer.getResourcesList()) {
							if (r.getName().equals("cpus") && r.getScalar().getValue() >= cpu) {
								cpuRole = r.getRole();
							} else if(r.getName().equals("mem") && r.getScalar().getValue() >= mem) {
								memRole = r.getRole();
							} else if(r.getName().equals("ports") && r.getScalar().getValue() >= mem) {
								for(Range range: r.getRanges().getRangeList()) {
									if(slot.getPort() >= range.getBegin() && slot.getPort() <= range.getEnd()) {
										portsRole = r.getRole();
										break;
									}
								}
							}
						}

                        String wgetCmds = "wget " + _configUrl + "/storm.yaml";

                        String executorDataStr = JSONValue.toJSONString(executorData);
                        LOG.info("Launching task with executor data: <" + executorDataStr + ">");
                        TaskInfo task = TaskInfo.newBuilder()
                            .setName("worker " + slot.getNodeId() + ":" + slot.getPort())
                            .setTaskId(TaskID.newBuilder()
                                    .setValue(MesosCommon.taskId(slot.getNodeId(), slot.getPort())))
                            .setSlaveId(offer.getSlaveId())
                            .setExecutor(ExecutorInfo.newBuilder()
                                    .setExecutorId(ExecutorID.newBuilder().setValue(details.getId()))
                                    .setData(ByteString.copyFromUtf8(executorDataStr))
                                    .setCommand(CommandInfo.newBuilder()
                                            .addUris(URI.newBuilder().setValue((String) _conf.get(CONF_EXECUTOR_URI)))
                                            .setValue("cd storm-mesos* && cd conf && rm -f storm.yaml && " + wgetCmds + " && cd .. && python bin/storm supervisor storm.mesos.MesosSupervisor")
                                    ))
                            .addResources(Resource.newBuilder()
                                    .setName("cpus")
                                    .setType(Type.SCALAR)
                                    .setScalar(Scalar.newBuilder().setValue(cpu))
                                    .setRole(cpuRole))
                            .addResources(Resource.newBuilder()
                                    .setName("mem")
                                    .setType(Type.SCALAR)
                                    .setScalar(Scalar.newBuilder().setValue(mem))
                                    .setRole(memRole))
                            .addResources(Resource.newBuilder()
                                    .setName("ports")
                                    .setType(Type.RANGES)
                                    .setRanges(Ranges.newBuilder()
                                            .addRange(Range.newBuilder()
                                                    .setBegin(slot.getPort())
                                                    .setEnd(slot.getPort())))
                                    .setRole(portsRole))
                            .build();
                        toLaunch.get(id).add(task);
                    }
                }
            }
            for(OfferID id: toLaunch.keySet()) {
                List<TaskInfo> tasks = toLaunch.get(id);

                LOG.info("Launching tasks for offer " + id.getValue() + "\n" + tasks.toString());
                _driver.launchTasks(id, tasks);

                _offers.remove(id);
            }
        }
    }

    public static void main(String[] args) {
        backtype.storm.daemon.nimbus.launch(new MesosNimbus());
    }
}
