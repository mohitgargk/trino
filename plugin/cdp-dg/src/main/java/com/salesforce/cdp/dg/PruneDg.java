/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.salesforce.cdp.dg;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.json.JSONArray;
import org.json.JSONObject;
import static io.airlift.slice.Slices.utf8Slice;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public final class PruneDg {

    private PruneDg() {}

    @Description("UDF to prune-dg ")
    @ScalarFunction("prune_dg")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice pruneDg(ConnectorSession session, @SqlType(StandardTypes.VARCHAR) Slice jsonValue, @SqlType(StandardTypes.VARCHAR) Slice jsonSchema)
    {
        try {
            long startTime = System.nanoTime();

            String sSchema = jsonSchema.toStringUtf8();
            String sValue = jsonValue.toStringUtf8();
            JSONObject schema = new JSONObject(sSchema);
            JSONObject document = new JSONObject(sValue);

            JSONObject reducedJson = reduceJson(document, schema.getJSONObject("properties"));
            System.out.println("Reduced JSON: " + reducedJson.toString());

            Slice toReturn = utf8Slice(reducedJson.toString());

            long endTime = System.nanoTime();
            System.out.println("time elapsed in microsecs = " + (endTime-startTime)/1000);
            System.out.println(reducedJson);
            return toReturn;

        }
        catch (Throwable t) {
            throwIfInstanceOf(t, Error.class);
            throwIfInstanceOf(t, TrinoException.class);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, t);
        }
    }

    private static JSONObject reduceJson(JSONObject json, JSONObject schemaProperties) {
        JSONObject reducedJson = new JSONObject();

        for (String key : schemaProperties.keySet()) {
            if (!json.has(key)) continue;

            Object schemaValue = schemaProperties.getJSONObject(key).get("type");
            if ("object".equals(schemaValue)) {
                JSONObject subObject = json.getJSONObject(key);
                JSONObject subSchemaProperties = schemaProperties.getJSONObject(key).getJSONObject("properties");
                reducedJson.put(key, reduceJson(subObject, subSchemaProperties));
            } else if ("array".equals(schemaValue)) {
                JSONArray array = json.getJSONArray(key);
                JSONArray outArray = new JSONArray();

                for(int i=0; i<array.length(); i++) {
                    JSONObject subObject = array.getJSONObject(i);
                    JSONObject subSchemaProperties = schemaProperties.getJSONObject(key).getJSONObject("items").getJSONObject("properties");
                    outArray.put(reduceJson(subObject, subSchemaProperties));

                }
                reducedJson.put(key, outArray );

            } else {
                reducedJson.put(key, json.get(key));
            }
        }

        return reducedJson;
    }


}
