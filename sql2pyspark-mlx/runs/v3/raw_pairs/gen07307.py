from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") == "COMPLETED") & F.col("channel").isin(["store", "partner"]) & (F.col("status") != "RETURNED"))
    .groupBy("status")
    .agg(F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"))
)
