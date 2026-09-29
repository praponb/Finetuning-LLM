from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["partner", "mobile"]) & (F.col("channel") == "store"))
    .groupBy("status")
    .agg(F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"))
)
