from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter(F.col("is_flagged") & (F.col("amount") >= 2100) & F.col("is_flagged"))
)

transactions_stats = (
    valid_transactions
    .groupBy("merchant", F.hour("txn_time").alias("txn_hour"))
    .agg(F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"))
)

result = (
    transactions_stats
    .select("merchant", "txn_hour", "txn_type_list")
    .orderBy("merchant")
)
