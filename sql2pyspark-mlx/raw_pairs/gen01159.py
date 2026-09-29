from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(6, 8))
    .groupBy(
        F.when(F.col("unit_price") >= 400, "high").when(F.col("unit_price") >= 170, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.countDistinct("unit_price").alias("unique_products"),
        F.sum("quantity").alias("total_quantity"),
    )
    .orderBy(F.col("total_quantity").desc())
)
