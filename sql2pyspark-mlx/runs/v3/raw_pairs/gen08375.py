from pyspark.sql import functions as F

result = (
    df_orders
    .select("amount")
    .union(df_subscriptions.select("monthly_fee"))
)
