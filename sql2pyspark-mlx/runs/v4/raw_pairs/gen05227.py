from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "LOAN"]))
    .select("status", F.lit("account").alias("source"))
    .unionByName(
        df_subscriptions
        .select("status", F.lit("subscription").alias("source"))
    )
    .distinct()
)
