from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id")
w_grp = Window.partitionBy("account_id").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w4 = Window.partitionBy(F.to_date("txn_time"))
w5 = Window.partitionBy("account_id", "status").orderBy(F.col("txn_time").desc_nulls_last(), "id")
w6 = Window.partitionBy("status").orderBy("txn_time", "id")
w7 = Window.partitionBy("status").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status").orderBy(F.col("burst_id").desc(), "id")

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & F.col("is_flagged"))
    .select("*", F.trim("txn_type").alias("clean_txn_type"))
)

with_windows = (
    base
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "clean_txn_type",
        F.lead("amount", 2).over(w).alias("next_amount"),
        F.row_number().over(w_grp).alias("seq_no"),
        F.row_number().over(w3).alias("event_seq"),
        F.round((F.col("amount") / F.sum("amount").over(w4)), 4).alias("amount_share"),
    )
)

joined = (
    with_windows
    .alias("s")
    .join(df_accounts.alias("b"), F.col("s.account_id") == F.col("b.id"), "left")
    .filter(F.col("b.account_type") != "LOAN")
    .select(
        "s.id",
        "s.account_id",
        "s.amount",
        "s.txn_time",
        "s.is_flagged",
        "s.next_amount",
        "s.event_seq",
        "s.amount_share",
        F.col("b.status"),
    )
)

latest = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

gaps = (
    latest
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w6) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "amount",
        "txn_time",
        "is_flagged",
        "next_amount",
        "amount_share",
        "status",
        "rank_in_group",
        F.sum("session_start_flag").over(w7).alias("burst_id"),
    )
)

ranked = (
    islands
    .withColumn("rank_in_group_2", F.row_number().over(w8))
    .filter(F.col("rank_in_group_2") == 1)
)

valid = (
    ranked
    .filter((F.col("txn_time") >= F.to_timestamp(F.lit("2023-10-13 05:00:00"))) & F.col("status").isNotNull())
    .select(
        "*",
        (F.col("rank_in_group") * F.col("burst_id")).alias("rank_in_group_times_burst_id"),
        F.substring("status", 1, 4).alias("status_prefix"),
    )
)

result = (
    valid
    .select("id", "status", "amount_share", "rank_in_group_times_burst_id")
    .unpivot(
        ids=["id", "status"],
        values=["amount_share", "rank_in_group_times_burst_id"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
)
