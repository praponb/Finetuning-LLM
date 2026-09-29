from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_count"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
)
