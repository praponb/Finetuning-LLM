from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter((F.col("amount") > 1600) | F.col("amount").between(2100, 2200))
)

summary = (
    valid_transactions
    .groupBy("account_id", "merchant")
    .agg(
        F.countDistinct("amount").alias("unique_merchants"),
        F.min("txn_time").alias("first_txn_time"),
    )
)

result = (
    summary
    .select("account_id", "merchant", "unique_merchants", "first_txn_time")
    .orderBy(F.col("merchant").desc(), "account_id")
    .limit(5)
)
