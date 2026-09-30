from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id", "product_id")
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
    )
)
