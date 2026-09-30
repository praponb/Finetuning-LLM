from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") < 1800) | F.col("txn_type").isin(["DEBIT", "CREDIT"]))
    .groupBy("txn_type", "is_flagged")
    .agg(F.max("amount").alias("max_amount"))
    .filter(F.col("max_amount") < 2000)
    .orderBy("max_amount")
)
