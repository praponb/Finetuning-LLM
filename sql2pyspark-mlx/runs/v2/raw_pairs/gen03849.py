from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([21, 28]))
    .select("status")
    .unionByName(df_subscriptions.select("status"))
)
