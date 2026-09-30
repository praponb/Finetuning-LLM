from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        "status",
        F.when(F.col("balance") >= 89000, "LARGE").otherwise("SMALL").alias("balance_tier"),
    )
    .agg(
        F.count_if(F.col("status") == "OPEN").alias("open_events"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("status") == "FROZEN", 1)), F.lit(0)), 2).alias("balance_per_frozen"),
        F.max(F.when(F.col("status") == "FROZEN", F.col("balance"))).alias("frozen_max_balance"),
        F.stddev_samp("balance").alias("balance_stddev"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("event_count").asc_nulls_last())
)
