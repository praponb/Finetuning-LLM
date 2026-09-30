from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w5 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w7 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(-3, Window.currentRow)

joined = (
    df_transactions
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "inner")
    .filter(F.col("p.balance") > 48000)
    .select(
        "a.id",
        "a.account_id",
        "a.txn_type",
        "a.amount",
        "a.txn_time",
        "a.merchant",
        F.col("p.opened_date"),
    )
)

gaps = (
    joined
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_transactions = gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

scored = (
    sessions_transactions
    .select(
        "*",
        F.sum("island_id").over(w3).alias("running_sum_island_id"),
        F.max("island_id").over(w3).alias("running_max_island_id"),
        F.avg("island_id").over(w3).alias("running_avg_island_id"),
        F.count("island_id").over(w3).alias("running_count_island_id"),
    )
)

gaps_2 = (
    scored
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w4).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w4) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = gaps_2.withColumn("island_id_2", F.sum("is_new_session").over(w5))

flagged = (
    sessionized
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w6).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w6) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "opened_date",
        "island_id",
        "running_sum_island_id",
        "running_count_island_id",
        F.sum("is_new_burst").over(w7).alias("session_no"),
    )
)

with_windows = (
    islands
    .withColumn("last4_avg_island_id", F.avg("island_id").over(w_running))
    .withColumn("last4_sum_island_id", F.sum("island_id").over(w_running))
    .withColumn("last4_max_island_id", F.max("island_id").over(w_running))
)

result = (
    with_windows
    .select(
        "id",
        "txn_type",
        "amount",
        "last4_avg_island_id",
        "last4_sum_island_id",
        "last4_max_island_id",
    )
    .unpivot(
        ids=["id", "txn_type"],
        values=["amount", "last4_avg_island_id", "last4_sum_island_id", "last4_max_island_id"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
