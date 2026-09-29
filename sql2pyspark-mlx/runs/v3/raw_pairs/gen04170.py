from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~((F.col("txn_time") < "2025-09-11") & ((F.to_date("txn_time") < "2024-08-01") | F.col("txn_type").isin(["FEE", "DEBIT"]))))
    .groupBy("merchant")
    .agg(F.avg("amount").alias("avg_amount"))
)
