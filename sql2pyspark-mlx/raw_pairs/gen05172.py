from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc())
w2 = Window.partitionBy("merchant").orderBy("txn_time_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w4 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("txn_type").orderBy(F.col("running_sum_txn_time_quarter").desc(), "id")
w_grp = Window.partitionBy("merchant").orderBy("is_new_burst", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("txn_type").orderBy("running_max_running_count_txn_time_quarter", "id")
w8 = Window.partitionBy("merchant", "txn_type").orderBy("running_sum_txn_time_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select("*", F.quarter("txn_time").alias("txn_time_quarter"))
)

latest = (
    filtered
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    latest
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        "rank_in_group",
        F.avg("txn_time_quarter").over(w2).alias("running_avg_txn_time_quarter"),
        F.max("txn_time_quarter").over(w2).alias("running_max_txn_time_quarter"),
        F.sum("txn_time_quarter").over(w2).alias("running_sum_txn_time_quarter"),
        F.count("txn_time_quarter").over(w2).alias("running_count_txn_time_quarter"),
    )
)

with_prev = metrics.withColumn("prev_txn_time", F.lag("txn_time").over(w3))

gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_txn_time").isNull() | (F.col("txn_time") > F.col("prev_txn_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id", F.sum("is_new_burst").over(w4))

ranked = (
    sessionized
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

enriched = (
    ranked
    .filter(F.hour("txn_time").between(8, 14))
    .select(
        "*",
        F.max("running_count_txn_time_quarter").over(w_grp).alias("running_max_running_count_txn_time_quarter"),
        F.count("running_count_txn_time_quarter").over(w_grp).alias("running_count_running_count_txn_time_quarter"),
        F.sum("running_count_txn_time_quarter").over(w_grp).alias("running_sum_running_count_txn_time_quarter"),
        F.avg("running_count_txn_time_quarter").over(w_grp).alias("running_avg_running_count_txn_time_quarter"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        "rank_in_group",
        "running_avg_txn_time_quarter",
        "running_max_txn_time_quarter",
        "running_sum_txn_time_quarter",
        "running_count_txn_time_quarter",
        "prev_txn_time",
        "is_new_burst",
        "session_id",
        "rn",
        "running_count_running_count_txn_time_quarter",
        "running_sum_running_count_txn_time_quarter",
        F.lag("running_max_running_count_txn_time_quarter").over(w7).alias("prev_running_max_running_count_txn_time_quarter"),
        F.lead("running_max_running_count_txn_time_quarter").over(w7).alias("next_running_max_running_count_txn_time_quarter"),
        F.lead("running_count_running_count_txn_time_quarter").over(w7).alias("next_running_count_running_count_txn_time_quarter"),
        F.lag("running_count_running_count_txn_time_quarter").over(w7).alias("prev_running_count_running_count_txn_time_quarter"),
        F.row_number().over(w7).alias("seq_no"),
        F.sum("running_sum_txn_time_quarter").over(w8).alias("running_running_sum_txn_time_quarter"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "txn_type",
        "merchant",
        "rank_in_group",
        "running_avg_txn_time_quarter",
        "running_max_txn_time_quarter",
        "running_sum_txn_time_quarter",
        "running_count_txn_time_quarter",
        "prev_txn_time",
        F.when(F.col("txn_type").rlike("^(REFUND|DEBIT)$"), "CORE").otherwise("GROUP_B").alias("txn_type_zone"),
    )
    .orderBy(F.col("running_avg_txn_time_quarter").asc_nulls_last())
)
