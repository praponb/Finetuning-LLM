from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & ((F.col("quantity") < 5) | (F.col("quantity") != 8)))
    .groupBy("order_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
    )
)
