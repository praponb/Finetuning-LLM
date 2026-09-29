from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([5, 28]))
    .select(
        "amount",
        "account_id",
        F.concat_ws("-", "merchant", "txn_type").alias("merchant_txn_type_key"),
        F.round((F.col("amount") * 1.07), 2).alias("amount_adjusted"),
    )
)
