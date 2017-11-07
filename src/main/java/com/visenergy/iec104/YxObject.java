package com.visenergy.iec104;

import com.flying.jdbc.SqlHelper;
import com.flying.jdbc.data.CommandType;
import com.flying.jdbc.data.Parameter;
import com.flying.jdbc.db.type.BaseTypes;
import com.flying.jdbc.util.DBConnection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.visenergy.iec104.util.RabbitMqUtils;
import net.sf.json.JSONObject;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by zhonghuan on 2017/7/25.
 */
public class YxObject {
    private Log log = LogFactory.getLog(YxObject.class);
    private static String RABBITMQ_QUEUE = "PV_YX";

    private String BUILDING_ID = "";
    private String INVERTER_ID = "";
    private String SERIAL = "";
    private int VERSION_FAIL=-1;        //软件版本不匹配
    private int SYSTEM_FAIL=-1;         //系统故障
    private int NBI_EXP_FAIL=-1;        //逆变电流异常
    private int CYI_FAIL=-1;            //残余电流异常
    private int WDGG_FAIL=-1;           //温度过高
    private int FS_FAIL=-1;             //风扇故障
    private int SPI_FAIL=-1;            //SPI通讯异常
    private int JYZKD_FAIL=-1;          //绝缘阻抗低
    private int AFCI_FAIL=-1;           //AFCI自检失败
    private int ZLDH_FAIL=-1;           //直流电弧故障
    private int ZC3_FAIL=-1;            //组串3反向
    private int LYBHQ_FAIL=-1;          //浪涌保护器故障
    private boolean flag=false;

    private Connection conn = null;
    private Channel channel = null;

    public YxObject(){
    }
    public YxObject(String inverterId, String buildingId, String serial){
        this.INVERTER_ID = inverterId;
        this.BUILDING_ID = buildingId;
        this.SERIAL = serial;

        //初始化rabbitmq
        try {
            this.getChannel();
        } catch (IOException e) {
            log.error("初始化rabbitMq失败",e);
        } catch (TimeoutException e) {
            log.error("初始化rabbitMq失败",e);
        }

        Runnable runnable = new Runnable() {

            public void run() {
                if(flag == true){
                    String sql =  "INSERT INTO T_PVMANAGE_INVERTER_FAILURE(FA_ID,FA_NAME,BUILDING_ID,INVERTER_ID,TIME) " +
                            "VALUES(?,?,?,?,?)";

                    DBConnection conn = SqlHelper.connPool.getConnection();

                    YxObject yxTable = new YxObject();
                    Class yx = (Class) yxTable.getClass();
                    Field[] fields = yx.getDeclaredFields();
                    String failureDescription = null;

                    for (int i = 0; i < fields.length ; i++) {
                        Field f = fields[i];
                        Object val;
                        String name = null;
                        f.setAccessible(true); //设置些属性是可以访问的
                        try {
                            name = f.getName();
                            val = f.get(yxTable);//得到此属性的值
                            if ("VERSION_FAIL".equals(name)){
                                failureDescription = "软件版本不匹配";
                            }else if ("SYSTEM_FAIL".equals(name)){
                                failureDescription = "系统故障";
                            }else if ("NBI_EXP_FAIL".equals(name)){
                                failureDescription = "逆变电流异常";
                            }else if ("CYI_FAIL".equals(name)){
                                failureDescription = "残余电流异常";
                            }else if ("WDGG_FAIL".equals(name)){
                                failureDescription = "温度过高";
                            }else if ("FS_FAIL".equals(name)){
                                failureDescription = "风扇故障";
                            }else if ("SPI_FAIL".equals(name)){
                                failureDescription = "SPI通讯异常";
                            }else if ("JYZKD_FAIL".equals(name)){
                                failureDescription = "绝缘阻抗低";
                            }else if ("AFCI_FAIL".equals(name)){
                                failureDescription = "AFCI自检失败";
                            }else if ("ZLDH_FAIL".equals(name)){
                                failureDescription = "直流电弧故障";
                            }else if ("ZC3_FAIL".equals(name)){
                                failureDescription = "组串3反向";
                            }else if ("LYBHQ_FAIL".equals(name)){
                                failureDescription = "浪涌保护器故障";
                            }else{
                                log.debug("遥信对象里的其他属性：" + name);
                            }

                            if (val != null){
                                if("0".equals(val.toString()) && failureDescription != null){
                                    Parameter[] params = new Parameter[5];

                                    String id = UUID.randomUUID().toString().replaceAll("-", "").toUpperCase();
                                    params[0] = new Parameter("ID", BaseTypes.VARCHAR,id);
                                    params[1] = new Parameter("FA_NAME", BaseTypes.VARCHAR,failureDescription);
                                    params[2] = new Parameter("BUILDING_ID", BaseTypes.VARCHAR,BUILDING_ID);
                                    params[3] = new Parameter("INVERTER_ID", BaseTypes.VARCHAR,INVERTER_ID);
                                    params[4] = new Parameter("TIME", BaseTypes.TIMESTAMP, new Timestamp(System.currentTimeMillis()));
                                    SqlHelper.executeNonQuery(conn, CommandType.Text, sql, params);
                                    log.debug("插入遥信故障信息到表中," + failureDescription);
                                }else {
                                    log.debug("遥信信息：" + failureDescription + "：false");
                                }
                            }
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                    }
                    clear();
                    SqlHelper.connPool.releaseConnection(conn);

                }else{
                    log.debug("未接收到数据");
                }

            }

        };
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        // 第二个参数为首次执行的延时时间，第三个参数为定时执行的间隔时间
        service.scheduleAtFixedRate(runnable, 20, 30, TimeUnit.SECONDS);
    }

    public void clear(){
        /*VERSION_FAIL=-1;
        SYSTEM_FAIL=-1;
        NBI_EXP_FAIL=-1;
        CYI_FAIL=-1;
        WDGG_FAIL=-1;
        FS_FAIL=-1;
        SPI_FAIL=-1;
        JYZKD_FAIL=-1;
        AFCI_FAIL=-1;
        ZLDH_FAIL=-1;
        ZC3_FAIL=-1;
        LYBHQ_FAIL=-1;*/
        this.flag = false;
    }

    public String getSERIAL() {
        return SERIAL;
    }

    public void setSERIAL(String SERIAL) {
        this.SERIAL = SERIAL;
    }

    public String getBUILDING_ID() {
        return BUILDING_ID;
    }

    public void setBUILDING_ID(String BUILDING_ID) {
        this.BUILDING_ID = BUILDING_ID;
    }

    public String getINVERTER_ID() {
        return INVERTER_ID;
    }

    public void setINVERTER_ID(String INVERTER_ID) {
        this.INVERTER_ID = INVERTER_ID;
    }

    public int getVERSION_FAIL() {
        return VERSION_FAIL;
    }

    public void setVERSION_FAIL(int VERSION_FAIL) {
        this.VERSION_FAIL = VERSION_FAIL;
        this.flag = true;
        this.sendRabbitMq("SERIAL","VERSION_FAIL", VERSION_FAIL);
    }

    public int getSYSTEM_FAIL() {
        return SYSTEM_FAIL;
    }

    public void setSYSTEM_FAIL(int SYSTEM_FAIL) {
        this.SYSTEM_FAIL = SYSTEM_FAIL;
        this.flag = true;
        this.sendRabbitMq("SERIAL","SYSTEM_FAIL", SYSTEM_FAIL);
    }

    public int getNBI_EXP_FAIL() {
        return NBI_EXP_FAIL;
    }

    public void setNBI_EXP_FAIL(int NBI_EXP_FAIL) {
        this.NBI_EXP_FAIL = NBI_EXP_FAIL;
        this.flag = true;
        this.sendRabbitMq("SERIAL","NBI_EXP_FAIL", NBI_EXP_FAIL);
    }

    public int getCYI_FAIL() {
        return CYI_FAIL;
    }

    public void setCYI_FAIL(int CYI_FAIL) {
        this.CYI_FAIL = CYI_FAIL;
        this.flag = true;
        this.sendRabbitMq("SERIAL","CYI_FAIL", CYI_FAIL);
    }

    public int getWDGG_FAIL() {
        return WDGG_FAIL;
    }

    public void setWDGG_FAIL(int WDGG_FAIL) {
        this.WDGG_FAIL = WDGG_FAIL;
        this.flag = true;
        this.sendRabbitMq("SERIAL","WDGG_FAIL", WDGG_FAIL);
    }

    public int getFS_FAIL() {
        return FS_FAIL;
    }

    public void setFS_FAIL(int FS_FAIL) {
        this.FS_FAIL = FS_FAIL;
        this.flag = true;
        this.sendRabbitMq("SERIAL","FS_FAIL", FS_FAIL);
    }

    public int getSPI_FAIL() {
        return SPI_FAIL;
    }

    public void setSPI_FAIL(int SPI_FAIL) {
        this.SPI_FAIL = SPI_FAIL;
        this.flag = true;
        this.sendRabbitMq("SERIAL","SPI_FAIL", SPI_FAIL);
    }

    public int getJYZKD_FAIL() {
        return JYZKD_FAIL;
    }

    public void setJYZKD_FAIL(int JYZKD_FAIL) {
        this.JYZKD_FAIL = JYZKD_FAIL;
        this.flag = true;
        this.sendRabbitMq("SERIAL","JYZKD_FAIL", JYZKD_FAIL);
    }

    public int getAFCI_FAIL() {
        return AFCI_FAIL;
    }

    public void setAFCI_FAIL(int AFCI_FAIL) {
        this.AFCI_FAIL = AFCI_FAIL;
        this.flag = true;
        this.sendRabbitMq("SERIAL","AFCI_FAIL", AFCI_FAIL);
    }

    public int getZLDH_FAIL() {
        return ZLDH_FAIL;
    }

    public void setZLDH_FAIL(int ZLDH_FAIL) {
        this.ZLDH_FAIL = ZLDH_FAIL;
        this.flag = true;
        this.sendRabbitMq("SERIAL","ZLDH_FAIL", ZLDH_FAIL);
    }

    public int getZC3_FAIL() {
        return ZC3_FAIL;
    }

    public void setZC3_FAIL(int ZC3_FAIL) {
        this.ZC3_FAIL = ZC3_FAIL;
        this.flag = true;
        this.sendRabbitMq("SERIAL","ZC3_FAIL", ZC3_FAIL);
    }

    public int getLYBHQ_FAIL() {
        return LYBHQ_FAIL;
    }

    public void setLYBHQ_FAIL(int LYBHQ_FAIL) {
        this.LYBHQ_FAIL = LYBHQ_FAIL;
        this.flag = true;
        this.sendRabbitMq("SERIAL","LYBHQ_FAIL", LYBHQ_FAIL);
    }

    public Connection getConn() throws IOException, TimeoutException {
        if(conn == null){
            conn = RabbitMqUtils.newConnection();
        }
        return conn;
    }

    public Channel getChannel() throws IOException, TimeoutException {
        if(channel == null){
            channel = getConn().createChannel();
        }
        return channel;
    }

    public void sendRabbitMq(String ID,String name,Object value){
        Map map = new HashedMap();
        map.put("name",ID);
        map.put("SERIAL",getSERIAL());
        map.put(name,value);
        sendRabbitMq("lightTopology","inverterData",map);
    }


    public void sendRabbitMq(String module,String subModule,Map dataMap){
        Map<String,Object> resultMap = new HashedMap();
        resultMap.put("module",module);
        resultMap.put("subModule",subModule);
        resultMap.put("data",dataMap);

        try {
            RabbitMqUtils.sendMq(channel,RABBITMQ_QUEUE, JSONObject.fromObject(resultMap).toString());
        } catch (IOException e) {
            log.error("RabbitMq传输消息失败",e);
        } catch (TimeoutException e) {
            log.error("RabbitMq传输消息失败",e);
        }
    }
}
