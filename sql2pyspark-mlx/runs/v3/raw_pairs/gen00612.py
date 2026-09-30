from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["mobile", "store", "partner"]) & F.month("order_date").isin([4, 7, 9]))
    .groupBy("status")
    .agg(F.min("shipping_fee").alias("min_shipping_fee"))
    .orderBy("status")
)
