from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([4, 8, 34]))
    .select(
        "amount",
        F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0).alias("is_debit"),
    )
)
