from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan").isNotNull())
    .select(F.lit("subscription").alias("kind"), "plan")
    .union(
        df_transactions
        .filter(F.col("merchant").isNotNull())
        .select(F.lit("transaction").alias("kind"), "merchant")
    )
    .distinct()
)
