package com.epam.bigdata.hive2;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

@UDFType(deterministic = false)
public class UserAgentParseUDF extends GenericUDTF {
    private static final String UA_TYPE = "UA_TYPE";
    private static final String UA_FAMILY = "UA_FAMILY";
    private static final String OS_NAME = "OS_NAME";
    private static final String DEVICE = "DEVICE";

    private PrimitiveObjectInspector agentOI;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldNames.add(UA_TYPE);
        fieldNames.add(UA_FAMILY);
        fieldNames.add(OS_NAME);
        fieldNames.add(DEVICE);
        for (int i = 0; i < 4; i++) {
            fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        }

        agentOI = (PrimitiveObjectInspector) argOIs[0];

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
                fieldOIs);

    }

    @Override
    public void process(Object[] arg) throws HiveException {
        String userAgentString = agentOI.getPrimitiveJavaObject(arg[0]).toString();
        forward(processUserAgentString(userAgentString).toArray());
    }

    private List<Object> processUserAgentString(String userAgentString) {
        ArrayList<Object> forwardList = new ArrayList<>();
        UserAgent ua = UserAgent.parseUserAgentString(userAgentString);
        forwardList.add(ua.getBrowser().getBrowserType().getName());
        forwardList.add(ua.getBrowser().getGroup().getName());
        forwardList.add(ua.getOperatingSystem().getName());
        forwardList.add(ua.getOperatingSystem().getDeviceType().getName());
        return forwardList;
    }
    @Override
    public void close() throws HiveException {

    }
}
