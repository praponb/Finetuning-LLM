from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged") | (F.col("txn_type") == "REFUND"))
    .select(
        "id",
        "account_id",
        "merchant",
        "txn_time",
        F.ceil("amount").alias("amount_whole"),
        F.to_date("txn_time").alias("txn_time_date"),
    )
    .orderBy("txn_time_date")
)
