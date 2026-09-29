from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type", "account_id").orderBy("island_id", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w5 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-05-21 14:00:00").cast("timestamp")) & F.col("account_id").isin([3, 14, 18, 39]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)

with_gaps = (
    cleaned
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_transactions = (
    with_gaps
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "is_flagged_label",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

with_windows = (
    sessions_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        "island_id",
        F.min("amount").over(w3).alias("last4_min_amount"),
        F.count("amount").over(w3).alias("last4_count_amount"),
        F.avg("amount").over(w3).alias("last4_avg_amount"),
    )
)

flagged = (
    with_windows
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w4) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("is_new_burst").over(w5))

result = (
    islands
    .groupBy("account_id")
    .agg(
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("last4_count_amount")).otherwise(0)).alias("fee_last4_count_amount"),
        F.count(F.when(F.col("merchant") == "Shell", 1)).alias("shell_rows"),
        F.count_if(F.col("merchant") == "Starbucks").alias("starbucks_events"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.round(F.avg("last4_avg_amount"), 2).alias("avg_last4_avg_amount"),
    )
    .filter(F.col("fee_last4_count_amount") > 0)
)
