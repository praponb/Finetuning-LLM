from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.countDistinct("duration_sec").alias("unique_event_types"),
        F.min("duration_sec").alias("min_duration_sec"),
        F.count("*").alias("total_count"),
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_count"),
    )
    .filter(F.col("product_count") >= 5)
    .drop("product_count")
)
