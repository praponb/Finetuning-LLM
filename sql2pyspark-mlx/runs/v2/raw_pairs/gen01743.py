from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page", F.date_format("event_time", "yyyy-MM").alias("event_month"))
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_count"),
    )
    .filter(F.col("product_count") > 3)
)
