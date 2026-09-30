from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id", "event_type")
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
    )
    .filter(F.col("row_count") >= 2)
    .orderBy("event_type", F.col("row_count").desc())
)
