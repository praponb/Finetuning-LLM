from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("monthly_fee", F.lit("subscription").alias("source"))
    .union(df_transactions.select("amount", F.lit("transaction").alias("source")))
    .distinct()
)
