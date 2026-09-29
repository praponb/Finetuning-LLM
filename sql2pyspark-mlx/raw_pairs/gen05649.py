from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") >= 4) & (F.col("quantity") == 4))
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 450, "high").when(F.col("unit_price") >= 160, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.sum("unit_price").alias("total_unit_price"),
    )
    .filter(F.col("avg_quantity") > 41)
    .orderBy(F.col("unit_price_bucket").desc())
)
