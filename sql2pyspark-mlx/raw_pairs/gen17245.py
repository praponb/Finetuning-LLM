from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(((F.col("quantity") >= 3) & (F.col("unit_price") <= 110)) | F.col("product_id").isNotNull())
    .groupBy(
        "order_id",
        F.when(F.col("unit_price") >= 410, "high").when(F.col("unit_price") >= 70, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.count("*").alias("total_count"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)
