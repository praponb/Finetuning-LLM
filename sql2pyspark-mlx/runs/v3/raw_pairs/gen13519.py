from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") < 39) & ((F.col("shipping_fee") >= 38) | (F.col("store_id") == 19)))
    .groupBy("store_id")
    .agg(F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"))
)
