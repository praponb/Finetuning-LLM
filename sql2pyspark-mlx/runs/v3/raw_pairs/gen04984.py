from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("monthly_fee")
    .union(df_orders.select("shipping_fee"))
)
