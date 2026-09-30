from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(100, 310))
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.min("unit_price").alias("min_unit_price"),
        F.count("unit_price").alias("unit_price_count"),
    )
    .filter(F.col("min_unit_price") > 170)
)
