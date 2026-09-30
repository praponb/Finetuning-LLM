from pyspark.sql import functions as F

recent_orders = (
    df_orders
    .filter((F.col("channel") != "web") & F.col("channel").isin(["mobile", "web", "store"]))
)

agg = (
    recent_orders
    .groupBy("status")
    .agg((F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"))
)

result = agg.filter(F.col("shipping_fee_range") > 54)
