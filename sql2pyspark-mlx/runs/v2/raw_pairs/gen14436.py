from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([6, 21, 24]))
    .groupBy("txn_type", "account_id")
    .agg(F.count(F.when(F.col("merchant") == "Grab", 1)).alias("grab_count"))
    .orderBy("grab_count", "txn_type", "account_id")
    .limit(5)
)
