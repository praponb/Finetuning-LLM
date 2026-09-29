from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isNotNull())
    .select(F.col("account_type").alias("label"))
    .unionByName(
        df_payments
        .filter(F.col("status").isNotNull())
        .select(F.col("status").alias("label"))
    )
)
