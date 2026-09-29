from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w4 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_account_id = df_transactions.groupBy("account_id").agg(F.min("amount").alias("min_transaction_amount"))

base = (
    df_transactions
    .filter((F.col("merchant").eqNullSafe(F.lit("Shell"))) & (F.col("txn_time") > F.lit("2023-07-16").cast("date")))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.col("txn_time").cast("date").alias("txn_time_day"),
    )
)

flagged = (
    base
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "txn_time_day",
        F.sum("new_group_flag").over(w2).alias("session_id"),
    )
)

cleaned = (
    islands
    .filter((F.col("txn_time") >= F.to_date(F.lit("2023-08-18"))) & (F.col("session_id") <= 7))
    .select(
        "*",
        F.length("txn_type").alias("txn_type_length"),
        F.substring("merchant", 1, 3).alias("merchant_prefix"),
    )
)

prev_events = cleaned.withColumn("prev_txn_time", F.lag("txn_time").over(w3))

gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands_transactions = (
    gaps
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "txn_time_day",
        "session_id",
        "txn_type_length",
        F.sum("new_group_flag").over(w4).alias("session_no"),
    )
)

wide = (
    islands_transactions
    .groupBy("account_id")
    .pivot("txn_type", ["CREDIT", "REFUND", "FEE"])
    .agg(F.sum("session_id"))
)

result = (
    wide
    .join(amount_by_account_id, on="account_id", how="left")
    .select("account_id", "FEE", F.col("min_transaction_amount"))
)
