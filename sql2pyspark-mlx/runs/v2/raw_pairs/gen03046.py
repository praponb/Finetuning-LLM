from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") <= 1000)
    .groupBy(F.quarter("txn_time").alias("txn_quarter"), "is_flagged", "account_id")
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("min_amount") < 2100)
    .orderBy("account_id")
)
