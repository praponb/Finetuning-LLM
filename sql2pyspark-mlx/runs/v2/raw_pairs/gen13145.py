from pyspark.sql import functions as F

filtered_transactions = df_transactions.filter(F.col("is_flagged") & F.col("is_flagged"))

transactions_stats = (
    filtered_transactions
    .groupBy("txn_type")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
    )
)

result = (
    transactions_stats
    .filter(F.col("stddev_amount") >= 48)
    .select("txn_type", "txn_type_list")
    .orderBy(F.col("txn_type").desc())
)
