from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").between(3, 8))
    .groupBy("order_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
    )
)
