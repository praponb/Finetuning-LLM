from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(2400, 3600) & (F.col("account_id") == 30))
    .select(
        "id",
        "amount",
        "account_id",
        "merchant",
        F.concat_ws("-", "txn_type", "merchant").alias("txn_type_merchant_key"),
    )
    .orderBy("amount", "account_id")
)
