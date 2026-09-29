from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").desc_nulls_first(), "id")
w_ordered = Window.partitionBy("account_type").orderBy("balance_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy(F.col("running_min_balance_scaled").asc_nulls_last())
w_recent = Window.partitionBy("customer_id").orderBy("running_min_balance_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_accounts
    .filter(F.col("status").isNotNull() & F.col("account_type").isin(["LOAN", "SAVINGS"]))
    .withColumn("balance_scaled", F.round((F.col("balance") * 0.85), 2))
    .withColumn("resolved_status", F.coalesce("status", F.lit("N/A")))
)

eligible = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.amount").between(1800, 3000)), "left_semi")
)

latest = (
    eligible
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    latest
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        "resolved_status",
        "row_num",
        F.min("balance_scaled").over(w_ordered).alias("running_min_balance_scaled"),
        F.sum("balance_scaled").over(w_ordered).alias("running_sum_balance_scaled"),
        F.avg("balance_scaled").over(w_ordered).alias("running_avg_balance_scaled"),
        F.max("balance_scaled").over(w_ordered).alias("running_max_balance_scaled"),
    )
)

eligible_accounts = (
    metrics
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_type").isin(["CREDIT", "FEE"])), "left_semi")
)

not_flagged = (
    eligible_accounts
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") <= F.col("s.opened_date")), "left_semi")
)

scored = (
    not_flagged
    .withColumn("running_min_balance_scaled_dense_rank", F.dense_rank().over(w_grp))
    .withColumn("cumulative_running_sum_balance_scaled", F.sum("running_sum_balance_scaled").over(w_recent))
)

result = (
    scored
    .groupBy(
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
        "customer_id",
    )
    .agg(
        F.count_if(F.col("status") == "OPEN").alias("open_events"),
        F.count_if(F.col("status") == "FROZEN").alias("frozen_events"),
        F.percentile_approx("running_max_balance_scaled", [0.25, 0.5, 0.75]).alias("running_max_balance_scaled_quartiles"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.percentile_approx("running_max_balance_scaled", [0.25, 0.5, 0.75]).alias("running_max_balance_scaled_quartiles_2"),
        F.size(F.collect_set("status")).alias("num_status_2"),
    )
)
