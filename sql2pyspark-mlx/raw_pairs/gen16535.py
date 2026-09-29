from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("session_no").desc())
w4 = Window.partitionBy(F.year("opened_date"), "account_id").orderBy("txn_time", "id")
w5 = Window.partitionBy("account_id").orderBy("session_no", "id")
w6 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_ordered = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.partitionBy(F.to_date("txn_time"), "account_id").orderBy(F.col("session_no_dense_rank").desc(), "id")
w9 = Window.partitionBy("account_id")
w10 = Window.partitionBy("account_type").orderBy("txn_time", "id")
w11 = Window.partitionBy("account_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_transactions
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.account_id",
        "s.amount",
        "s.txn_time",
        "s.is_flagged",
        F.col("p.opened_date"),
    )
)

with_gaps = (
    with_dims
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "is_flagged",
        "opened_date",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

with_gaps_2 = (
    islands
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_transactions = (
    with_gaps_2
    .select(
        "id",
        "account_id",
        "txn_time",
        "is_flagged",
        "opened_date",
        F.sum("is_new_burst").over(w2).alias("session_no"),
    )
)

scored = (
    sessions_transactions
    .select(
        "id",
        "account_id",
        "txn_time",
        "is_flagged",
        "session_no",
        F.dense_rank().over(w3).alias("session_no_dense_rank"),
        F.lead("session_no", 2, 0).over(w4).alias("next_session_no"),
        F.ntile(10).over(w5).alias("session_no_bucket"),
        F.count("session_no").over(w6).alias("session_no_count_90d"),
    )
)

scored_transactions = (
    scored
    .select(
        "id",
        "account_id",
        "txn_time",
        "session_no",
        "session_no_bucket",
        "session_no_count_90d",
        F.avg("next_session_no").over(w_ordered).alias("next_session_no_avg_2"),
        F.ntile(10).over(w8).alias("session_no_dense_rank_bucket"),
        F.round((F.col("session_no_count_90d") / F.sum("session_no_count_90d").over(w9)), 4).alias("session_no_count_90d_share"),
    )
)

with_dims_transactions = (
    scored_transactions
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "inner")
    .filter(F.col("d.customer_id").isin([3, 5, 16, 37]))
    .select("a.*", F.col("d.opened_date"), F.col("d.status"), F.col("d.account_type"))
)

gaps = (
    with_dims_transactions
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w10) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands_transactions = gaps.withColumn("session_id", F.sum("session_start_flag").over(w11))

result = (
    islands_transactions
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["account_status", "transaction_status"], ["account_status"]],
        F.col("p.status").alias("account_status"),
        F.col("a.status").alias("transaction_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("a.session_no_count_90d_share")).alias("total_session_no_count_90d_share"),
        F.grouping_id().alias("level_id"),
    )
)
