from pyspark.sql import functions as F

kept = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") < "2025-09-01"), "left_anti")
)

result = (
    kept
    .rollup(F.date_trunc("WEEK", "opened_date").alias("week_start"))
    .agg(
        F.sum("balance").alias("total_balance"),
        F.countDistinct("account_type").alias("distinct_account_types"),
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("status") == "CLOSED", 1)), F.lit(0)), 2).alias("balance_per_closed"),
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance"))).alias("frozen_total_balance"),
        F.percentile_approx("balance", 0.9).alias("balance_p90"),
    )
)
