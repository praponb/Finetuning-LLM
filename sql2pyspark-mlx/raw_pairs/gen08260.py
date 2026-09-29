from pyspark.sql import functions as F

base = df_web_events.filter(F.col("session_id").isin([9, 11, 20]))

web_events_stats = (
    base
    .groupBy("event_type")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.sum(F.when(F.col("page") == "/checkout", 1).otherwise(0)).alias("checkout_count"),
    )
)

result = (
    web_events_stats
    .filter(F.col("avg_duration_sec") < 41)
    .orderBy(F.col("checkout_count").desc(), "event_type")
    .limit(5)
)
