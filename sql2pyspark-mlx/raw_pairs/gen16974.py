from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
    .orderBy(F.col("avg_unit_price").desc())
)
