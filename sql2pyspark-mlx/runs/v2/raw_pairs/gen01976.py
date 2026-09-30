from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(4, 6))
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
        F.when(F.col("unit_price") >= 340, "high").when(F.col("unit_price") >= 130, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.countDistinct("unit_price").alias("unique_orders"),
    )
    .orderBy("product_id", "quantity_bucket", "unit_price_bucket")
    .limit(1)
)
