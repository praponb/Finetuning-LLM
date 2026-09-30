from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "account_id").orderBy("is_flagged_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w3 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("txn_type", "status").orderBy(F.col("running_count_is_flagged_flag").desc(), "id")
w5 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w6 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter((F.col("txn_type") == "DEBIT") & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("merchant_filled"),
    )
)

enriched = (
    valid
    .select(
        "*",
        F.count("is_flagged_flag").over(w).alias("running_count_is_flagged_flag"),
        F.min("is_flagged_flag").over(w).alias("running_min_is_flagged_flag"),
        F.sum("is_flagged_flag").over(w).alias("running_sum_is_flagged_flag"),
    )
)

with_dims = (
    enriched
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.status"), F.col("b.opened_date"))
)

with_gaps = (
    with_dims
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w2).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w2) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w3))

deduped = (
    islands
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

with_gaps_2 = (
    deduped
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w5) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = (
    with_gaps_2
    .select(
        "id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        "running_count_is_flagged_flag",
        "running_sum_is_flagged_flag",
        "status",
        "opened_date",
        "burst_id",
        "rn",
        F.sum("session_start_flag").over(w6).alias("burst_id_2"),
    )
)

with_gaps_3 = (
    sessionized
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w2) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands_transactions = with_gaps_3.withColumn("session_id", F.sum("is_new_session").over(w3))

result = (
    islands_transactions
    .rollup(
        F.when(F.col("burst_id") >= 8, "LARGE").otherwise("SMALL").alias("burst_id_tier"),
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
        "merchant",
    )
    .agg(
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("burst_id"))).alias("credit_total_burst_id"),
        F.avg(F.when(F.col("status") == "CLOSED", F.col("burst_id"))).alias("closed_avg_burst_id"),
        F.count_if(F.col("txn_type") == "REFUND").alias("refund_events"),
    )
    .filter(F.col("refund_events") > 1)
)
