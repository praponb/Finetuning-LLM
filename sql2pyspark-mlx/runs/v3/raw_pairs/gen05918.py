from pyspark.sql import functions as F

filtered_transactions = df_transactions.filter(~F.col("merchant").isin(["7-Eleven", "Shell"]))

summary = (
    filtered_transactions
    .groupBy("txn_type")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = summary.select("txn_type", "amount_range")
