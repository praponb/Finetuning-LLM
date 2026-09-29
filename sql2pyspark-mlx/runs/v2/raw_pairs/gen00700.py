from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").isNull())
    .select("status", F.lit("account").alias("source"))
    .unionByName(
        df_subscriptions
        .filter(F.month("end_date").isin([2, 6, 8]))
        .select("status", F.lit("subscription").alias("source"))
    )
)
