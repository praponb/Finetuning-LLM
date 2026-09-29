from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.quarter("event_time").alias("event_quarter"))
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"))
    .orderBy("page_values", "event_quarter")
    .limit(10)
)
