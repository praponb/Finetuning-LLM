from pyspark.sql import functions as F

base = df_web_events.filter((F.col("session_id") == 19) & (F.col("page") == "/checkout"))

web_events_stats = (
    base
    .groupBy(
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
    )
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
)

result = (
    web_events_stats
    .select("event_cohort", "duration_sec_count", "avg_duration_sec")
    .orderBy("avg_duration_sec")
)
