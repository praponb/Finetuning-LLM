from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id")
w2 = Window.partitionBy("customer_id", "account_type").orderBy("opened_date", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("account_type")
w4 = Window.partitionBy("status").orderBy("account_type_stddev_samp_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("account_type", "status").orderBy(F.col("account_type_stddev_samp_balance").asc_nulls_last())
w6 = Window.partitionBy("status").orderBy(F.col("account_type_stddev_samp_balance").asc_nulls_last(), "id")
w_grp = Window.partitionBy("status").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("customer_id").orderBy("balance_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_accounts
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

eligible = (
    latest
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_type") == "DEBIT"), "left_semi")
)

with_windows = (
    eligible
    .select(
        "id",
        "customer_id",
        "account_type",
        "status",
        F.avg("balance").over(w2).alias("balance_avg_5"),
        F.stddev_samp("balance").over(w3).alias("account_type_stddev_samp_balance"),
    )
)

kept = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.merchant").isin(["Shell", "Lazada"])), "left_anti")
)

with_windows_accounts = (
    kept
    .withColumn("last_known_account_type_stddev_samp_balance", F.last("account_type_stddev_samp_balance", ignorenulls=True).over(w4))
    .withColumn("account_type_stddev_samp_balance_cume_dist", F.cume_dist().over(w5))
)

ranked = (
    with_windows_accounts
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    ranked
    .filter(F.col("status") == "OPEN")
    .select(
        "id",
        "customer_id",
        "status",
        "balance_avg_5",
        "account_type_stddev_samp_balance",
        "last_known_account_type_stddev_samp_balance",
        F.last("account_type_stddev_samp_balance_cume_dist", ignorenulls=True).over(w_grp).alias("last_known_account_type_stddev_samp_balance_cume_dist"),
        F.sum("account_type_stddev_samp_balance_cume_dist").over(w_hist).alias("cumulative_account_type_stddev_samp_balance_cume_dist"),
    )
)

grouped = (
    metrics
    .groupBy("customer_id")
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.countDistinct("status").alias("distinct_status"),
        F.round(F.sum("account_type_stddev_samp_balance") / F.nullif(F.count(F.when(F.col("status") == "FROZEN", 1)), F.lit(0)), 2).alias("account_type_stddev_samp_balance_per_frozen"),
        F.size(F.collect_set("status")).alias("num_status_2"),
        F.sum("balance_avg_5").alias("total_balance_avg_5"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
)

result = (
    grouped
    .select(
        "customer_id",
        "num_status",
        "distinct_status",
        "account_type_stddev_samp_balance_per_frozen",
        "num_status_2",
        "total_balance_avg_5",
        "distinct_customer_ids",
    )
    .orderBy("num_status")
)
