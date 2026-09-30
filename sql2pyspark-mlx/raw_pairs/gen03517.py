from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id") == 6)
    .groupBy("txn_type")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .filter(F.col("amount_range") >= 25)
    .orderBy("txn_type", F.col("amount_range").desc())
)
