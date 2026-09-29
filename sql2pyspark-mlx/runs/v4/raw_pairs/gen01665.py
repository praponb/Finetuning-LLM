from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isNull())
    .select("status", F.lit("account").alias("source"))
    .unionByName(
        df_subscriptions
        .select("status", F.lit("subscription").alias("source"))
    )
    .distinct()
    .orderBy(F.col("status").desc())
)
