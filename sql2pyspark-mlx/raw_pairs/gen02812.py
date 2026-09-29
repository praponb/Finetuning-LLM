from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([3, 20, 25, 27]) & F.col("quantity").between(3, 4) & F.col("quantity").between(3, 4))
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.count("unit_price").alias("unit_price_count"),
    )
    .filter(F.col("unit_price_count") < 2)
    .drop("unit_price_count")
    .orderBy(F.col("stddev_quantity").desc(), "order_id")
    .limit(5)
)
