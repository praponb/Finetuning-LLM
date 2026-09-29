from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("account_id", "merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("merchant").orderBy("last_known_amount_per_cumulative_amount", "id")
w5 = Window.partitionBy("merchant").orderBy("cumulative_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("first_known_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("last_known_amount", "id")
w8 = Window.partitionBy("merchant").orderBy("txn_time", "id")

scored = (
    df_transactions
    .filter(F.col("merchant") == "Lazada")
    .select(
        "id",
        "txn_time",
        "merchant",
        "is_flagged",
        F.sum("amount").over(w).alias("cumulative_amount"),
        F.first("amount", ignorenulls=True).over(w_hist).alias("first_known_amount"),
        F.last("amount", ignorenulls=True).over(w_running).alias("last_known_amount"),
    )
)

valid = (
    scored
    .filter((F.col("txn_time") > F.lit("2023-10-07 07:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .withColumn("last_known_amount_per_cumulative_amount", F.round((F.col("last_known_amount") / F.col("cumulative_amount")), 2))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("UNKNOWN")))
)

metrics = (
    valid
    .withColumn("next_cumulative_amount", F.lead("cumulative_amount", 2, 0).over(w4))
    .withColumn("last_known_last_known_amount", F.last("last_known_amount", ignorenulls=True).over(w5))
    .withColumn("first_known_first_known_amount", F.first("first_known_amount", ignorenulls=True).over(w6))
    .withColumn("event_seq", F.row_number().over(w7))
)

flagged = (
    metrics
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w8) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "txn_time",
        "merchant",
        "is_flagged",
        "cumulative_amount",
        "first_known_amount",
        "last_known_amount",
        "merchant_filled",
        "last_known_last_known_amount",
        "first_known_first_known_amount",
        "event_seq",
        F.sum("is_new_burst").over(w_running).alias("session_no"),
    )
)

with_gaps = (
    sessionized
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w8).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w8) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "txn_time",
        "merchant",
        "is_flagged",
        "cumulative_amount",
        "first_known_amount",
        "last_known_amount",
        "merchant_filled",
        "first_known_first_known_amount",
        "session_no",
        F.sum("is_new_burst").over(w_running).alias("session_no_2"),
    )
)

result = (
    islands
    .select(
        "id",
        "merchant",
        "cumulative_amount",
        "first_known_amount",
        "last_known_amount",
        "merchant_filled",
        "first_known_first_known_amount",
        "session_no",
        "session_no_2",
    )
    .orderBy(F.col("last_known_amount").desc_nulls_last())
)
