from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") <= 3)
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
    .orderBy(F.col("order_id").desc())
)
