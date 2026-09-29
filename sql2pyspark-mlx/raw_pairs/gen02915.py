from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter(F.col("status").eqNullSafe(F.lit("FROZEN")))
    .select(
        "*",
        F.length("status").alias("status_length"),
        F.concat("account_type", F.lit(" "), "status").alias("account_type_status"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("balance") >= 65000, "LARGE").otherwise("SMALL").alias("balance_tier"),
        "customer_id",
    )
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.stddev_samp("balance").alias("balance_stddev"),
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("status") == "CLOSED", 1)), F.lit(0)), 2).alias("balance_per_closed"),
        F.countDistinct("status").alias("distinct_status_2"),
        F.percentile_approx("balance", 0.9).alias("balance_p90"),
    )
)
