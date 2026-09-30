from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged") & (F.col("txn_type") == "DEBIT"))
    .groupBy("account_id")
    .agg(F.min("amount").alias("min_amount"))
    .orderBy("min_amount")
)
