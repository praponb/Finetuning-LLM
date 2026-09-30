from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("store_id")
    .pivot("status", ["RETURNED", "CANCELLED"])
    .agg(F.avg("shipping_fee").alias("value_sum"), F.count(F.lit(1)).alias("n"))
)
