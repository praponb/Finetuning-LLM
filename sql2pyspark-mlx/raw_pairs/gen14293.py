from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant", F.month("txn_time").alias("txn_month"))
    .agg(
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("amount")).otherwise(0)).alias("amazon_amount"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("amazon_amount") > 41)
)
