from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") >= 37)
    .select("shipping_fee")
    .unionByName(df_subscriptions.select(F.col("monthly_fee").alias("shipping_fee")))
    .distinct()
)
