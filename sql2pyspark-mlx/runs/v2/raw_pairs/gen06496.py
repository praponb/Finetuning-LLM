from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") > 449)
    .groupBy("page")
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
    .filter(F.col("duration_sec_count") < 5)
)
