from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") <= 63000)
    .select("status", F.lit("account").alias("source"))
    .unionByName(
        df_subscriptions
        .select("status", F.lit("subscription").alias("source"))
    )
    .distinct()
    .orderBy(F.col("status").desc())
)
