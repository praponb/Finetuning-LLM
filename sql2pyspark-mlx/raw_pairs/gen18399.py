from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("account_id").orderBy("session_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("txn_type", "account_type").orderBy("txn_time", "id")
w5 = Window.partitionBy("status").orderBy(F.col("running_max_balance").desc(), "id")

valid = (
    df_transactions
    .filter(F.col("account_id").isin([21, 23, 24, 25]))
    .select(
        "*",
        F.trim("merchant").alias("clean_merchant"),
        F.concat_ws(",", "merchant", "txn_type").alias("merchant_txn_type_key"),
        F.coalesce("merchant", F.lit("OTHER")).alias("resolved_merchant"),
    )
)

joined = (
    valid
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "left")
    .select("e.*", F.col("a.balance"), F.col("a.account_type"))
)

flagged = (
    joined
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("session_start_flag").over(w2))

with_windows = (
    islands
    .select(
        "id",
        "account_id",
        "merchant",
        "clean_merchant",
        "merchant_txn_type_key",
        "balance",
        "account_type",
        F.max("balance").over(w_recent).alias("running_max_balance"),
        F.min("balance").over(w_recent).alias("running_min_balance"),
        F.avg("balance").over(w_recent).alias("running_avg_balance"),
        F.lag("amount", 1).over(w4).alias("prev_amount"),
    )
)

enriched = (
    with_windows
    .alias("src")
    .join(df_accounts.alias("a"), F.col("src.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.account_type").isin(["CREDIT", "CHECKING", "SAVINGS"]))
    .select(
        "src.*",
        F.col("a.opened_date"),
        F.col("a.balance").alias("account_balance"),
        F.col("a.status"),
    )
)

latest = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .rollup(
        F.when(F.col("running_min_balance") >= 70000, "PREMIUM").otherwise("STANDARD").alias("running_min_balance_tier"),
        F.date_trunc("YEAR", "opened_date").alias("year_start"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance")).otherwise(0)).alias("frozen_balance"),
        F.avg(F.when(F.col("status") == "CLOSED", F.col("running_max_balance"))).alias("closed_avg_running_max_balance"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"),
    )
    .filter(F.col("frozen_balance") > 0)
)
