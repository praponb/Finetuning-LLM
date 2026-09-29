from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([2, 14]))
    .groupBy("order_id")
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
    .orderBy(F.col("avg_unit_price").desc(), "stddev_quantity", "order_id")
    .limit(5)
)
