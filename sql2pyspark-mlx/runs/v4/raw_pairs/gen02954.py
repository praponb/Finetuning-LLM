from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(13, 27))
    .select("shipping_fee")
    .intersect(df_subscriptions.select(F.col("monthly_fee").alias("shipping_fee")))
    .orderBy("shipping_fee")
)
