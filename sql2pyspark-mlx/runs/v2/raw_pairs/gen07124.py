from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.max("event_time").alias("last_event_time"),
    )
    .filter(F.col("duration_sec_count") < 9)
    .select("page", "duration_sec_count", "last_event_time")
    .orderBy(F.col("duration_sec_count").desc())
)
