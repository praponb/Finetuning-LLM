from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([34, 35]))
    .select("status")
    .intersect(df_subscriptions.select("status"))
)
