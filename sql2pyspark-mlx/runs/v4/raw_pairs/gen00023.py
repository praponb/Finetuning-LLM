from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([3, 37]))
    .groupBy("product_id")
    .agg(
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.max("unit_price").alias("max_unit_price"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)
