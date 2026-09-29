from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.date_trunc("quarter", "opened_date").alias("quarter_start"),
        F.date_trunc("week", "opened_date").alias("week_start"),
    )
    .agg(
        F.stddev_samp("balance").alias("balance_stddev"),
        F.countDistinct("account_type").alias("distinct_account_types"),
        F.percentile_approx("balance", 0.95).alias("balance_p95"),
        F.sum(F.when(F.col("status") == "OPEN", F.col("balance")).otherwise(0)).alias("open_balance"),
    )
    .filter(F.col("balance_stddev") < 1000000)
)
