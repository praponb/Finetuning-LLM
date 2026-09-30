from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("product_id").isin([10, 15]) & F.col("unit_price").isNotNull() & (F.col("product_id") == 25))
)

summary = (
    base
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.countDistinct("unit_price").alias("unique_orders"),
    )
)

result = summary.orderBy(F.col("avg_unit_price").desc())
