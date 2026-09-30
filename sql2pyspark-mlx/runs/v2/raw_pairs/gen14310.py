from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") >= 76000)
    .select("status", F.lit("account").alias("source"))
    .unionByName(
        df_subscriptions
        .filter(F.col("end_date") >= "2025-01-03")
        .select("status", F.lit("subscription").alias("source"))
    )
    .orderBy("source")
)
