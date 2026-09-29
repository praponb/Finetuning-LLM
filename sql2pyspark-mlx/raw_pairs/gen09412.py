from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time_period", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy("txn_time_period", "id")
w4 = Window.partitionBy("txn_type").orderBy("txn_time_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w6 = Window.partitionBy("txn_type").orderBy("next_island_id", "id")

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (~F.col("merchant").eqNullSafe(F.lit("Shell"))))
    .withColumn("txn_time_period", F.date_trunc("week", "txn_time"))
    .withColumn("txn_type_merchant_key", F.concat_ws("-", "txn_type", "merchant"))
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("OTHER")))
)

flagged = (
    base
    .withColumn("session_start_flag", F.when(F.col("txn_time_period") - F.lag("txn_time_period").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("session_start_flag").over(w2))

flagged_2 = (
    sessionized
    .withColumn("is_new_session", F.when(F.col("txn_time_period") - F.lag("txn_time_period").over(w3) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = (
    flagged_2
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        "txn_time_period",
        "txn_type_merchant_key",
        "session_start_flag",
        "island_id",
        F.sum("is_new_session").over(w4).alias("island_id_2"),
    )
)

enriched = (
    islands
    .filter(F.col("island_id_2").between(2, 5))
    .select(
        "*",
        F.lead("island_id").over(w_hist).alias("next_island_id"),
        F.lag("session_start_flag").over(w_hist).alias("prev_session_start_flag"),
        F.lead("session_start_flag").over(w_hist).alias("next_session_start_flag"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)

latest = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "txn_type",
        "is_flagged",
        "txn_time_period",
        "txn_type_merchant_key",
        "island_id",
        "next_island_id",
        "prev_session_start_flag",
        "next_session_start_flag",
    )
)

result = (
    latest
    .groupBy(F.date_trunc("QUARTER", "txn_time_period").alias("quarter_start"))
    .agg(
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.round(F.avg("next_island_id"), 2).alias("avg_next_island_id"),
    )
    .filter(F.col("avg_next_island_id") >= 0)
    .orderBy(F.col("avg_next_island_id").desc())
)
