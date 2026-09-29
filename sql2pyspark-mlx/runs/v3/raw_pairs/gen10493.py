from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isin(["Amazon", "Starbucks"]))
    .withColumn("is_refund", F.when(F.col("txn_type") == "REFUND", 1).otherwise(0))
    .withColumn("clean_merchant", F.trim("merchant"))
)
