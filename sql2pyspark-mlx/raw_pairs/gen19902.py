from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id")
w4 = Window.partitionBy("txn_type", "merchant")
w5 = Window.partitionBy("merchant").orderBy("amount", "id")
w6 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w7 = Window.orderBy("island_id")
w8 = Window.partitionBy("merchant").orderBy("prev_txn_time", "id")
w9 = Window.partitionBy("merchant").orderBy("prev_txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("account_id", "txn_type").orderBy(F.col("session_start_flag").desc_nulls_last(), "id")

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & F.hour("txn_time").between(9, 12))
    .select("*", F.lower("merchant").alias("merchant_lower"))
)

prev_events = valid.withColumn("prev_txn_time", F.lag("txn_time").over(w))

gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

with_windows = (
    islands
    .select(
        "*",
        F.sum("amount").over(w3).alias("account_id_sum_amount"),
        F.max("session_start_flag").over(w4).alias("txn_type_max_session_start_flag"),
        F.row_number().over(w5).alias("seq_no"),
        F.sum("session_start_flag").over(w6).alias("session_start_flag_sum_30d"),
        F.percent_rank().over(w7).alias("island_id_percent_rank"),
    )
)

flagged = (
    with_windows
    .withColumn("is_new_session", F.when(F.col("prev_txn_time") - F.lag("prev_txn_time").over(w8) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands_transactions = flagged.withColumn("session_no", F.sum("is_new_session").over(w9))

deduped = (
    islands_transactions
    .withColumn("row_num", F.row_number().over(w10))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        "merchant_lower",
        "prev_txn_time",
        "session_start_flag",
        "island_id",
        F.when(F.col("merchant").rlike("^(7-Eleven|Grab)$"), "CORE").otherwise("OTHER_ZONE").alias("merchant_zone"),
    )
)
