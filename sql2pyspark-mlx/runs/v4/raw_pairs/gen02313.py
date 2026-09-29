from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("balance")).otherwise(0)).alias("checking_balance"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.count_if(F.col("status") == "OPEN").alias("open_events"),
        F.percentile_approx("balance", 0.9).alias("balance_p90"),
    )
    .filter(F.col("open_events") < 1)
    .orderBy(F.col("open_events").desc())
)
