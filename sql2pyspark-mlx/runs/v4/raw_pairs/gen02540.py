from pyspark.sql import functions as F

valid = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2024-04-21 06:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .withColumn("merchant_lower", F.lower("merchant"))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("NONE")))
)

result = (
    valid
    .select("account_id", "txn_type")
    .exceptAll(
        df_transactions
        .filter(F.col("amount") < 2400)
        .select("account_id", "txn_type")
    )
)
