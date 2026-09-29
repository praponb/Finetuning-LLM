from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.sum("unit_price").alias("total_unit_price"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.countDistinct("quantity").alias("unique_orders"),
    )
    .filter(F.col("unique_orders") < 1)
    .drop("unique_orders")
    .orderBy("stddev_unit_price", "product_id")
)
