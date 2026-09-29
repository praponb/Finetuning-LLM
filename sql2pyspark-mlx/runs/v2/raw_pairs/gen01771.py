from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(6, 8))
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.count("unit_price").alias("unit_price_count"),
        F.countDistinct("quantity").alias("unique_orders"),
    )
)
