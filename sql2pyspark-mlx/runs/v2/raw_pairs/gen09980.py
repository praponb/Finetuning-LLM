from pyspark.sql import functions as F

filtered_transactions = df_transactions.filter(F.col("txn_type") == "CREDIT")

merchant_summary = (
    filtered_transactions
    .groupBy("merchant")
    .agg(F.count(F.when(F.col("merchant") == "Shell", 1)).alias("shell_count"))
)

result = (
    merchant_summary
    .filter(F.col("shell_count") >= 9)
    .orderBy(F.col("shell_count").desc())
)
