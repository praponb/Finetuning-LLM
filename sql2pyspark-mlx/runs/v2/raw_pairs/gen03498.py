from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "REFUND")
    .groupBy("txn_type")
    .pivot("merchant", ["Grab", "Amazon"])
    .agg(F.sum("amount"))
)
