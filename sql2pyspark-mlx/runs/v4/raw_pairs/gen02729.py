from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.hour("txn_time").between(12, 15))
    .groupBy("account_id")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .filter(F.col("amount_range") < 61)
    .select("account_id", "amount_range")
)
