from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date")
    .union(df_orders.select("order_date"))
)
