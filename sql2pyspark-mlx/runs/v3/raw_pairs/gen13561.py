from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("monthly_fee", F.lit("subscription").alias("source"))
    .union(
        df_transactions
        .filter(F.col("merchant").isNull())
        .select("amount", F.lit("transaction").alias("source"))
    )
)
