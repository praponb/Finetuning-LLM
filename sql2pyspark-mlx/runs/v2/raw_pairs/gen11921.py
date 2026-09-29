from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") > 180) & F.col("unit_price").between(190, 320))
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.avg("unit_price").alias("avg_unit_price"),
        F.max("unit_price").alias("max_unit_price"),
    )
    .orderBy(F.col("product_id").desc())
)
