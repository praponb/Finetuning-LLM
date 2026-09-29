from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["DEBIT", "CREDIT"]) & (F.col("txn_type") == "FEE") & F.hour("txn_time").between(10, 16))
    .groupBy("merchant")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .orderBy("amount_range")
)
