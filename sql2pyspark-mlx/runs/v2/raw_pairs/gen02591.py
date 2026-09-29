from pyspark.sql import functions as F

result = (
    df_web_events
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_count"),
    )
    .orderBy(F.col("avg_duration_sec").desc())
)
