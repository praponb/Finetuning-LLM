from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date")
    .intersect(df_orders.select("order_date"))
    .orderBy("start_date")
)
