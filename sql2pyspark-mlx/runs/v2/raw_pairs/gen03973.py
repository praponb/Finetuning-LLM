from pyspark.sql import functions as F

recent_web_events = (
    df_web_events
    .filter(F.col("session_id").isin([11, 34]))
    .withColumn("event_time_period", F.date_trunc("YEAR", "event_time"))
)

agg = (
    recent_web_events
    .groupBy("event_type", "session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/checkout", 1).otherwise(0)).alias("checkout_count"),
    )
)

result = (
    agg
    .filter(F.col("checkout_count") >= 29)
    .select("event_type", "session_id", "checkout_count")
)
