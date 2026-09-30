from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("txn_type").isin(["DEBIT", "REFUND"]))
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .orderBy(F.col("amount_range").desc())
)
