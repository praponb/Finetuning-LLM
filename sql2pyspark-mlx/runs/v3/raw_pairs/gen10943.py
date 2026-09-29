from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") != "standard")
    .select("monthly_fee")
    .intersect(df_orders.select(F.col("shipping_fee").alias("monthly_fee")))
)
