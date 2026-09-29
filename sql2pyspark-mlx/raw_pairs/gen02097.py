from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(F.lit("subscription").alias("source"), "monthly_fee")
    .union(df_transactions.select(F.lit("transaction").alias("source"), "amount"))
)
