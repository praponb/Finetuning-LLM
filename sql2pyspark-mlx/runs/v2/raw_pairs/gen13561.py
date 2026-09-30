from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(F.col("session_id").isin([7, 11, 16]) & (F.col("event_time") < "2025-01-01"))
    .withColumn("duration_sec_filled", F.coalesce("duration_sec", F.lit(0)))
)

web_events_stats = (
    base
    .groupBy("page")
    .agg(
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
        F.count("*").alias("web_event_count"),
    )
)

result = web_events_stats.filter(F.col("web_event_count") >= 9)
