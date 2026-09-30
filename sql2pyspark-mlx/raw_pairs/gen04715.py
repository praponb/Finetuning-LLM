from pyspark.sql import functions as F

base = df_transactions.filter(F.col("account_id").isin([3, 12, 38, 39]))

transactions_stats = (
    base
    .groupBy(
        "txn_type",
        F.when(F.col("amount") >= 3700, "high").when(F.col("amount") >= 1200, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.max("txn_time").alias("last_txn_time"),
    )
)

result = transactions_stats.orderBy("txn_type", "last_txn_time")
