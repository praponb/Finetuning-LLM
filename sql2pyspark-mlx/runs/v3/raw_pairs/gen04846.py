from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") >= 240) & F.col("product_id").isNotNull())
    .groupBy(
        F.when(F.col("unit_price") >= 360, "high").when(F.col("unit_price") >= 190, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.avg("unit_price").alias("avg_unit_price"))
    .orderBy(F.col("avg_unit_price").desc())
)
