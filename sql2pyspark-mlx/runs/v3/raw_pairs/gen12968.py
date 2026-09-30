from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").isNotNull() & (F.col("order_id") == 3) & F.col("order_id").isin([10, 13, 35, 36]))
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.min("quantity").alias("min_quantity"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
    .orderBy("min_quantity", "order_id")
    .limit(10)
)
