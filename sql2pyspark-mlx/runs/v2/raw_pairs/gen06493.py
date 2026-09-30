from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([20, 22, 25]))
    .select("status")
    .unionByName(df_subscriptions.select("status"))
)
