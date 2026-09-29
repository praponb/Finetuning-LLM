from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([14, 34]) & (F.col("txn_type") != "REFUND"))
    .select(
        "id",
        "account_id",
        "txn_type",
        F.round("amount", 1).alias("amount_rounded"),
    )
)
