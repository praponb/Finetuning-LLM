from pyspark.sql import functions as F

kept = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") < F.col("s.opened_date")), "left_anti")
)

result = (
    kept
    .groupBy(F.date_trunc("QUARTER", "opened_date").alias("quarter_start"))
    .agg(
        F.size(F.collect_set("account_type")).alias("num_account_types"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.stddev_samp("balance").alias("balance_stddev"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"),
    )
    .filter(F.col("balance_stddev") >= 0)
    .orderBy(F.col("quarter_start").asc_nulls_last())
)
