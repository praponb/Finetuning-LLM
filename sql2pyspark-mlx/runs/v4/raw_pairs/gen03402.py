from pyspark.sql import functions as F

matched_keys = (
    df_accounts
    .select("account_type", "status")
    .exceptAll(
        df_accounts
        .filter(F.col("opened_date") < "2024-12-01")
        .select("account_type", "status")
    )
)

kept_rows = (
    df_accounts
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("customer_id")
    .agg(
        F.avg(F.when(F.col("account_type") == "CHECKING", F.col("balance"))).alias("checking_avg_balance"),
        F.stddev_samp("balance").alias("balance_stddev"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.percentile_approx("balance", 0.9).alias("balance_p90"),
        F.stddev_samp("balance").alias("balance_stddev_2"),
        F.stddev_samp("balance").alias("balance_stddev_3"),
    )
    .orderBy(F.col("balance_stddev_2").desc())
)
