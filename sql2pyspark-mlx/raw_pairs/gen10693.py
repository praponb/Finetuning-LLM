from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-07-30").cast("date")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
)

joined = (
    cleaned
    .alias("src")
    .join(df_accounts.alias("p"), F.col("src.account_id") == F.col("p.id"), "inner")
    .filter(F.col("p.balance").between(42000, 61000))
    .select("src.*", F.col("p.opened_date"), F.col("p.status"))
)

with_dims = (
    joined
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "inner")
    .select(
        "s.*",
        F.col("a.account_type"),
        F.col("a.opened_date").alias("account_opened_date"),
    )
)

prev_events = with_dims.withColumn("prev_txn_time", F.lag("txn_time").over(w))

gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

with_gaps = (
    sessions_transactions
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_transactions_2 = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_transactions_2
    .groupBy(F.date_trunc("MONTH", "account_opened_date").alias("activity_month"))
    .agg(
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_rows"),
        F.round(F.avg("is_new_session"), 2).alias("avg_is_new_session"),
        F.countDistinct("status").alias("distinct_status"),
        F.sum(F.when(F.col("status") == "CLOSED", F.col("is_new_session")).otherwise(0)).alias("closed_is_new_session"),
    )
    .filter(F.col("distinct_status") > 1)
    .orderBy(F.col("avg_is_new_session").desc())
)
