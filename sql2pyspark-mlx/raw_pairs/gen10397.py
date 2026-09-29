from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page", F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(
        F.max("event_time").alias("latest_event_time"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_count"),
    )
    .orderBy(F.col("event_month").desc(), "page")
    .limit(20)
)
