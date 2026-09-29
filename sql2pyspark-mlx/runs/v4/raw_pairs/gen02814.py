from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("product_id") == 38) & F.col("order_id").isin([7, 17, 22, 23]))
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.sum("quantity").alias("total_quantity"),
    )
    .orderBy(F.col("quantity_bucket").desc())
)
