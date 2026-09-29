from pyspark.sql import functions as F

base = df_transactions.filter((F.col("txn_time") < "2025-04-22") & F.col("is_flagged"))

transactions_stats = (
    base
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"))
)

result = transactions_stats.select("txn_month", "txn_type_list")
