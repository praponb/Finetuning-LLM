from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(F.col("monthly_fee").alias("value"), F.lit("subscription").alias("source"))
    .unionByName(
        df_transactions
        .filter(F.col("amount").isNotNull())
        .select(F.col("amount").alias("value"), F.lit("transaction").alias("source"))
    )
    .orderBy("source")
)
