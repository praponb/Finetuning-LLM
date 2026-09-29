from pyspark.sql import functions as F

result = (
    df_transactions
    .select("amount")
    .intersect(df_subscriptions.select(F.col("monthly_fee").alias("amount")))
    .orderBy("amount")
)
