from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Shell", "Grab", "Starbucks"]) | (F.col("txn_time") < "2023-07-31"))
    .groupBy("account_id")
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") < 3)
    .orderBy("cnt", "account_id")
    .limit(10)
)
