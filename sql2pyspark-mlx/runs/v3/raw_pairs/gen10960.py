from pyspark.sql import functions as F

base = df_web_events.filter(F.col("event_time") < "2024-03-01")

summary = (
    base
    .groupBy("event_type")
    .agg(
        F.max("event_time").alias("last_event_time"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
)

result = (
    summary
    .filter(F.col("duration_sec_count") < 6)
    .select("event_type", "last_event_time", "duration_sec_count")
    .orderBy("last_event_time", F.col("event_type").desc())
)
