from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type", "merchant").orderBy("weeks_since_txn_time", "id")
w_ordered = Window.partitionBy("txn_type").orderBy("prev_weeks_since_txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.month("txn_time"), "txn_type").orderBy("txn_time", "id").rowsBetween(-2, -1)
w7 = Window.partitionBy("merchant").orderBy("burst_id", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w9 = Window.partitionBy("txn_type").orderBy("weeks_since_txn_time", "id")

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        (F.datediff(F.current_date(), "txn_time") / 7).alias("weeks_since_txn_time"),
        F.round("amount", 1).alias("amount_rounded"),
    )
)

with_gaps = (
    valid
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

scored = (
    islands
    .filter(F.col("is_flagged"))
    .withColumn("next_amount_rounded", F.lead("amount_rounded").over(w3))
    .withColumn("prev_weeks_since_txn_time", F.lag("weeks_since_txn_time").over(w3))
    .withColumn("row_seq", F.row_number().over(w3))
)

scored_transactions = (
    scored
    .withColumn("running_min_amount_rounded", F.min("amount_rounded").over(w_ordered))
    .withColumn("running_avg_amount_rounded", F.avg("amount_rounded").over(w_ordered))
    .withColumn("running_sum_amount_rounded", F.sum("amount_rounded").over(w_ordered))
    .withColumn("running_count_amount_rounded", F.count("amount_rounded").over(w_ordered))
    .withColumn("running_row_seq", F.sum("row_seq").over(w5))
)

result = (
    scored_transactions
    .select(
        "id",
        "amount",
        "txn_time",
        "burst_id",
        "prev_weeks_since_txn_time",
        "running_min_amount_rounded",
        "running_avg_amount_rounded",
        "running_count_amount_rounded",
        F.avg("burst_id").over(w6).alias("burst_id_avg_2_prior"),
        F.avg("running_avg_amount_rounded").over(w7).alias("running_avg_amount_rounded_avg_2"),
        F.sum("amount").over(w8).alias("amount_sum_7d"),
        F.row_number().over(w9).alias("row_seq_2"),
    )
)
