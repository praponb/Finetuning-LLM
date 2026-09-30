from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_time") >= "2025-04-01") & (F.col("txn_type") == "CREDIT"))
    .groupBy("is_flagged")
    .agg(
        F.max("amount").alias("max_amount"),
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("amount")).otherwise(0)).alias("amazon_amount"),
    )
)
