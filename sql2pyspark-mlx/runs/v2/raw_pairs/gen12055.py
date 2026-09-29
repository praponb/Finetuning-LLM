from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") > 280) & (F.col("quantity") != 6) & (F.col("unit_price") <= 230))
    .groupBy("product_id")
    .agg(F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"))
)
