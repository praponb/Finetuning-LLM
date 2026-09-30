from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([7, 13, 30, 35]))
    .select("status")
    .unionByName(df_subscriptions.filter(F.col("monthly_fee") < 220).select("status"))
    .orderBy("status")
)
