from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.to_date("txn_time"), "merchant").orderBy(F.col("amount").desc())
w4 = Window.partitionBy("merchant").orderBy("amount", "id")
w5 = Window.partitionBy("merchant").orderBy(F.col("event_seq").desc(), "id")
w6 = Window.partitionBy("merchant").orderBy("event_seq", "id")
w7 = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")

filtered = (
    df_transactions
    .filter(~F.col("merchant").isin(["Shell", "Lazada", "Amazon"]))
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.trim("txn_type").alias("clean_txn_type"),
    )
)

with_gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_transactions = (
    with_gaps
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "clean_txn_type",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

with_windows = (
    sessions_transactions
    .filter(F.col("merchant").isin(["Grab", "Starbucks"]))
    .withColumn("amount_cume_dist", F.cume_dist().over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
)

prev_events = with_windows.withColumn("prev_txn_time", F.lag("txn_time").over(w))

gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "session_id",
        "event_seq",
        F.sum("is_new_session").over(w2).alias("session_id_2"),
    )
)

cleaned = (
    islands
    .filter((F.col("txn_time") > F.lit("2023-10-11").cast("date")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "amount",
        "merchant",
        "session_id",
        "event_seq",
        "session_id_2",
        (F.col("event_seq") * F.col("amount")).alias("event_seq_times_amount"),
    )
)

ranked = (
    cleaned
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

with_windows_transactions = (
    ranked
    .select(
        "*",
        F.ntile(10).over(w6).alias("event_seq_bucket"),
        F.row_number().over(w7).alias("event_seq_2"),
        F.ntile(10).over(w5).alias("event_seq_bucket_2"),
    )
)

result = (
    with_windows_transactions
    .select(
        "id",
        "amount",
        "event_seq",
        "session_id_2",
        "event_seq_times_amount",
        "event_seq_bucket_2",
    )
    .orderBy(F.col("event_seq_times_amount").desc())
)
