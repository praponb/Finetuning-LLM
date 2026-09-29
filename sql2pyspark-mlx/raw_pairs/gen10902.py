from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.to_date("event_time").alias("event_day"))
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
    )
    .filter(F.col("max_duration_sec") > 390)
    .select("event_day", "max_duration_sec", "page_values")
    .orderBy("max_duration_sec", "page_values", "event_day")
    .limit(5)
)
