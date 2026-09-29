from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") < 1330)
    .select("status")
    .unionByName(df_subscriptions.select("status"))
)
