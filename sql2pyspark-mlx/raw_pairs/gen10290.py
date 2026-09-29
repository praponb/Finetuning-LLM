from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("account_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.to_date("txn_time") < "2025-07-18"))
    .withColumn("amount_band", F.when(F.col("amount") >= 4000, "High").when(F.col("amount") >= 1200, "Medium").otherwise("Low"))
    .withColumn("txn_time_hour", F.hour("txn_time"))
    .withColumn("merchant_clean", F.coalesce("merchant", F.lit("OTHER")))
)

with_gaps = (
    base
    .withColumn("is_new_session", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "account_id",
        "txn_time",
        "is_flagged",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

joined = (
    islands
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.account_type"))
)

with_dims = (
    joined
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.account_id",
        "a.account_type",
        F.col("b.account_type").alias("account_account_type"),
    )
)

with_dims_transactions = (
    with_dims
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "left")
    .filter(F.col("b.opened_date") <= "2023-08-01")
    .select("a.*", F.col("b.balance"), F.col("b.status"))
)

enriched = (
    with_dims_transactions
    .withColumn("running_sum_balance", F.sum("balance").over(w_running))
    .withColumn("running_min_balance", F.min("balance").over(w_running))
    .withColumn("running_max_balance", F.max("balance").over(w_running))
)

joined_transactions = (
    enriched
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "inner")
    .select(
        "s.id",
        "s.account_id",
        "s.account_type",
        "s.balance",
        "s.running_sum_balance",
        F.col("a.status").alias("account_status"),
        F.col("a.balance").alias("account_balance"),
        F.col("a.opened_date"),
    )
)

grouped = (
    joined_transactions
    .groupBy("account_id")
    .agg(
        F.avg("running_sum_balance").alias("avg_running_sum_balance"),
        F.percentile_approx("running_sum_balance", [0.25, 0.5, 0.75]).alias("running_sum_balance_quartiles"),
        F.percentile_approx("running_sum_balance", [0.25, 0.5, 0.75]).alias("running_sum_balance_quartiles_2"),
        F.count("*").alias("row_count"),
        F.round(F.sum("account_balance") / F.nullif(F.count(F.when(F.col("account_type") == "LOAN", 1)), F.lit(0)), 2).alias("account_balance_per_loan"),
        F.max("account_balance").alias("max_account_balance"),
    )
)

result = (
    grouped
    .select(
        "account_id",
        "avg_running_sum_balance",
        "running_sum_balance_quartiles",
        "running_sum_balance_quartiles_2",
    )
    .orderBy(F.col("account_id").desc())
    .limit(10)
)
