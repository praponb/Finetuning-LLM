from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").isNotNull())
    .groupBy(
        F.when(F.col("unit_price") >= 380, "high").when(F.col("unit_price") >= 120, "mid").otherwise("low").alias("unit_price_bucket"),
        "product_id",
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.count("quantity").alias("quantity_count"))
    .orderBy("quantity_count")
)
