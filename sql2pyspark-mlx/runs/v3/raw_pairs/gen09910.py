from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([10, 31, 33]) & (F.col("merchant") == "Shell") & (F.col("amount") > 3800))
    .select(
        "txn_type",
        "merchant",
        "amount",
        F.round((F.col("amount") * 0.9), 2).alias("amount_adjusted"),
    )
)
