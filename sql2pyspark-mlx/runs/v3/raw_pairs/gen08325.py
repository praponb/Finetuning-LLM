from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id", "txn_type")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum(F.when(F.col("merchant") == "Lazada", F.col("amount")).otherwise(0)).alias("lazada_amount"),
    )
    .filter(F.col("avg_amount") > 67)
    .orderBy("account_id", "txn_type")
    .limit(10)
)
