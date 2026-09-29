from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
    )
    .filter(F.col("avg_quantity") >= 4.5)
)
