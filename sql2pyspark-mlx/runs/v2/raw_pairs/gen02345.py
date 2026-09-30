from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([11, 17, 39]))
    .groupBy("txn_type")
    .pivot("merchant", ["Lazada", "Starbucks", "Shell"])
    .agg(F.max("amount"))
    .orderBy("txn_type")
)
