from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "click") & ((F.col("session_id") == 4) | (F.col("duration_sec") != 389)))
    .groupBy("event_type", F.quarter("event_time").alias("event_quarter"), "session_id")
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"))
)
