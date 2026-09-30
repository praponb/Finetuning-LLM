from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").isNotNull())
    .select("status", F.lit("account").alias("source"))
    .unionByName(
        df_payments
        .filter(F.col("method") == "bank_transfer")
        .select("status", F.lit("payment").alias("source"))
    )
    .distinct()
)
