from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(7, 10) & (F.col("duration_sec") >= 331))
    .groupBy("page")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.max("event_time").alias("latest_event_time"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") < 1)
)
