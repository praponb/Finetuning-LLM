from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(5, 6) | (F.col("quantity") >= 4))
    .groupBy(
        F.when(F.col("unit_price") >= 430, "high").when(F.col("unit_price") >= 180, "mid").otherwise("low").alias("unit_price_bucket"),
        "product_id",
    )
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.max("unit_price").alias("max_unit_price"),
        F.avg("quantity").alias("avg_quantity"),
    )
)
