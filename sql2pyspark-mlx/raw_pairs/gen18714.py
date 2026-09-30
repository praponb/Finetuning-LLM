from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type").isin(["FEE", "DEBIT"]))
    .groupBy("is_flagged")
    .agg(F.max("amount").alias("max_amount"))
)
