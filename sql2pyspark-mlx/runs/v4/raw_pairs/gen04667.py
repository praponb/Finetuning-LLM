from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([1, 15, 26, 32]))
    .groupBy("txn_type")
    .pivot("merchant", ["Shell", "Grab"])
    .agg(F.sum("amount"))
)
