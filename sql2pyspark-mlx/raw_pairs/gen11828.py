from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("txn_type").orderBy("island_id", "id")
w4 = Window.partitionBy("merchant").orderBy("session_start_flag", "id")
w5 = Window.orderBy("prev_session_start_flag", "id")
w6 = Window.partitionBy("merchant")
w7 = Window.partitionBy("merchant").orderBy(F.col("next_session_start_flag").asc_nulls_first())
w8 = Window.orderBy("next_session_start_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter((F.col("merchant") == "Starbucks") & (F.col("txn_time") > F.lit("2023-05-29 10:00:00").cast("timestamp")))
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.concat_ws("-", "merchant", "txn_type").alias("merchant_txn_type_key"),
    )
)

flagged = (
    cleaned
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_transactions = flagged.withColumn("island_id", F.sum("session_start_flag").over(w2))

enriched = (
    sessions_transactions
    .withColumn("prev_session_start_flag", F.lag("session_start_flag").over(w_ordered))
    .withColumn("next_island_id", F.lead("island_id").over(w_ordered))
    .withColumn("prev_island_id", F.lag("island_id").over(w_ordered))
    .withColumn("event_seq", F.row_number().over(w_ordered))
    .withColumn("next_session_start_flag", F.lead("session_start_flag", 1, 0).over(w4))
)

scored = (
    enriched
    .select(
        "*",
        F.row_number().over(w5).alias("seq_no"),
        F.min("prev_session_start_flag").over(w6).alias("merchant_min_prev_session_start_flag"),
        F.rank().over(w7).alias("next_session_start_flag_rank"),
        F.ntile(10).over(w5).alias("prev_session_start_flag_bucket"),
        F.sum("next_session_start_flag").over(w8).alias("cumulative_next_session_start_flag"),
    )
)

result = (
    scored
    .select(
        "id",
        "account_id",
        "txn_time",
        "is_flagged",
        "merchant_txn_type_key",
        "session_start_flag",
        "island_id",
        "next_island_id",
        "prev_island_id",
    )
)
