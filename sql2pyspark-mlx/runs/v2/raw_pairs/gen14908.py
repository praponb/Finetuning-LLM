from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status", F.lit("account").alias("source"))
    .unionByName(
        df_subscriptions
        .filter(F.col("end_date").between("2025-02-01", "2025-05-01"))
        .select("status", F.lit("subscription").alias("source"))
    )
    .distinct()
)
