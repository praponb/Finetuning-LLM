from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([16, 22, 40]))
    .groupBy(
        "order_id",
        F.when(F.col("unit_price") >= 400, "high").when(F.col("unit_price") >= 140, "mid").otherwise("low").alias("unit_price_bucket"),
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.avg("quantity").alias("avg_quantity"),
    )
)
