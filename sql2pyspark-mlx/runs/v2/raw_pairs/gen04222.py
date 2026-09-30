from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") < "2025-10-03")
    .groupBy("merchant", F.hour("txn_time").alias("txn_hour"))
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_credit"),
        F.count("*").alias("transaction_count"),
    )
    .filter(F.col("transaction_count") > 3)
    .drop("transaction_count")
    .orderBy("txn_hour", "merchant")
    .limit(50)
)
