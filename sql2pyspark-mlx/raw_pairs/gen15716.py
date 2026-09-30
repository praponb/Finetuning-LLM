from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
        F.sum(F.when(F.col("page") == "/checkout", F.col("duration_sec")).otherwise(0)).alias("checkout_duration_sec"),
    )
    .orderBy("checkout_duration_sec", "event_type")
    .limit(1)
)
