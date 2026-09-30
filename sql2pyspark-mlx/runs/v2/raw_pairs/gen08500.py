from pyspark.sql import functions as F

result = (
    df_transactions
    .select(
        "account_id",
        F.concat_ws("-", "txn_type", "merchant").alias("txn_type_merchant_key"),
        F.ceil("amount").alias("amount_whole"),
    )
    .orderBy("amount_whole")
)
