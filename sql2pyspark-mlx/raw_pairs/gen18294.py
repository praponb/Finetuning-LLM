from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.sum(F.when(F.col("page") == "/cart", 1).otherwise(0)).alias("cart_count"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
    .orderBy(F.col("cart_count").desc())
)
