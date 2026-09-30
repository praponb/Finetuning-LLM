from pyspark.sql import functions as F

valid_orders = (
    df_orders
    .filter(~((F.col("discount") >= 0.25) & (F.col("shipping_fee") < 22) & (F.col("channel") != "store")))
)

agg = (
    valid_orders
    .groupBy("channel")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)

result = (
    agg
    .filter(F.col("stddev_amount") > 60)
    .select("channel", "stddev_amount")
    .orderBy("stddev_amount")
)
