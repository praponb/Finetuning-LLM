from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([28, 30]))
    .groupBy("account_id", "merchant", "txn_type")
    .agg(
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("debit_amount"),
    )
)
