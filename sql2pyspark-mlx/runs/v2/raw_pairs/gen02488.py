from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["CREDIT", "FEE"]) | F.col("account_id").isin([2, 10, 33, 40]))
    .select("id", "txn_time")
)
