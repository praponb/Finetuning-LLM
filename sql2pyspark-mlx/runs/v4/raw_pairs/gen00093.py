from pyspark.sql import functions as F

result = (
    df_accounts
    .select("opened_date", F.lit("account").alias("source"))
    .union(
        df_subscriptions
        .filter(F.col("status") == "CANCELLED")
        .select("end_date", F.lit("subscription").alias("source"))
    )
    .orderBy(F.col("source").desc())
)
