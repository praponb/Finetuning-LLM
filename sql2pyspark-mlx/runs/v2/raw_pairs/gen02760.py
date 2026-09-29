from pyspark.sql import functions as F

valid_web_events = (
    df_web_events
    .filter(F.col("event_type").isin(["search", "add_to_cart", "click"]) | (F.col("event_time") < "2023-10-01"))
)

summary = (
    valid_web_events
    .groupBy(F.month("event_time").alias("event_month"))
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
    )
)

result = (
    summary
    .filter(F.col("avg_duration_sec") >= 66)
    .orderBy(F.col("event_month").desc())
)
