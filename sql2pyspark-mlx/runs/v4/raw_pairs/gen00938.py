from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 28)
    .select("status")
    .unionByName(df_subscriptions.select("status"))
)
