from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type", "account_id")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum(F.when(F.col("merchant") == "Grab", F.col("amount")).otherwise(0)).alias("grab_amount"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("amount_count") >= 5)
)
