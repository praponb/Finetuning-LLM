from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([1, 14]))
    .select(
        "id",
        "amount",
        "account_id",
        "txn_time",
        F.round((F.col("amount") * 0.85), 2).alias("amount_adjusted"),
    )
    .orderBy(F.col("account_id").desc(), "id")
    .limit(3)
)
