from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w4 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("amount") < 1900))
    .withColumn("txn_type_title", F.initcap(F.trim("txn_type")))
)

with_gaps = (
    valid
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

filtered = (
    sessionized
    .filter((F.col("txn_time") >= F.lit("2023-07-28").cast("date")) & (~F.col("merchant").eqNullSafe(F.lit("Lazada"))))
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "txn_type_title",
        "is_new_burst",
        "burst_id",
        F.datediff(F.current_date(), "txn_time").alias("days_since_txn_time"),
    )
)

with_gaps_2 = (
    filtered
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w3) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_transactions = with_gaps_2.withColumn("session_no", F.sum("session_start_flag").over(w4))

filtered_transactions = (
    sessions_transactions
    .filter(F.col("is_flagged") & F.col("merchant").isNotNull())
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        "txn_type_title",
        "days_since_txn_time",
        "session_start_flag",
        "session_no",
        F.col("days_since_txn_time").cast("string").alias("days_since_txn_time_num"),
    )
)

gaps = (
    filtered_transactions
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w3).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w3) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized_transactions = gaps.withColumn("session_no_2", F.sum("is_new_burst").over(w4))

result = (
    sessionized_transactions
    .groupBy(
        F.when(F.col("session_start_flag") >= 1, "HIGH").otherwise("LOW").alias("session_start_flag_tier"),
        F.when(F.col("days_since_txn_time") >= 87, "HIGH").otherwise("LOW").alias("days_since_txn_time_tier"),
    )
    .agg(
        F.count(F.when(F.col("txn_type") == "REFUND", 1)).alias("refund_rows"),
        F.min("session_start_flag").alias("min_session_start_flag"),
        F.round(F.avg("session_start_flag"), 2).alias("avg_session_start_flag"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.count_if(F.col("txn_type") == "DEBIT").alias("debit_events"),
        F.avg(F.when(F.col("txn_type") == "REFUND", F.col("amount"))).alias("refund_avg_amount"),
    )
    .filter(F.col("min_session_start_flag") > 0)
    .orderBy(F.col("days_since_txn_time_tier").desc())
)
