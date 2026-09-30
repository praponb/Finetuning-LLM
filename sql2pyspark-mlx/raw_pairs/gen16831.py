from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type", "account_id")
    .pivot("merchant", ["Lazada", "Starbucks"])
    .agg(F.max("amount").alias("value_sum"))
)
