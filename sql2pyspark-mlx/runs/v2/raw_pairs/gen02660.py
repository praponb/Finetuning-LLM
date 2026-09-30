from pyspark.sql import functions as F

recent_transactions = df_transactions.filter(F.col("is_flagged") & (F.col("txn_time") < "2023-12-01"))

merchant_summary = (
    recent_transactions
    .groupBy("merchant")
    .agg(F.max("amount").alias("max_amount"))
)

result = merchant_summary.orderBy("max_amount")
