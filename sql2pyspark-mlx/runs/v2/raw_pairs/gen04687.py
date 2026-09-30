from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id") == 8) & (F.col("event_time") >= "2023-05-01"))
    .groupBy("event_type", F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
    )
    .orderBy("event_type")
)
