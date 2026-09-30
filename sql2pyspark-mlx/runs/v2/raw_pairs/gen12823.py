from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(F.col("page") != "/search")
    .withColumn("event_time_period", F.date_trunc("week", "event_time"))
)

web_events_stats = (
    base
    .groupBy(F.quarter("event_time_period").alias("event_period_quarter"))
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
)

result = (
    web_events_stats
    .filter(F.col("duration_sec_range") < 78)
    .select("event_period_quarter", "duration_sec_range")
    .orderBy(F.col("duration_sec_range").desc(), "event_period_quarter")
    .limit(10)
)
