from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([2, 22, 23]))
    .groupBy(F.quarter("txn_time").alias("txn_quarter"))
    .agg(F.max("amount").alias("max_amount"))
    .filter(F.col("max_amount") >= 2000)
    .orderBy(F.col("txn_quarter").desc())
    .limit(5)
)
