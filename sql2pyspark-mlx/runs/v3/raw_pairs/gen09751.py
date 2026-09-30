from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(173, 258))
    .groupBy(
        F.month("event_time").alias("event_month"),
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
    )
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.max("event_time").alias("last_event_time"),
    )
    .filter(F.col("avg_duration_sec") < 43)
    .select("event_month", "event_cohort", "avg_duration_sec", "last_event_time")
    .orderBy(F.col("event_month").desc())
)
