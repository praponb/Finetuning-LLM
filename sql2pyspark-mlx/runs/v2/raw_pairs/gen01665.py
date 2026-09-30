from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/product", "/checkout"]))
    .groupBy("event_type", F.quarter("event_time").alias("event_quarter"), "page")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
)
