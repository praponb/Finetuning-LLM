from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.stddev_samp("balance").alias("balance_stddev"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.max(F.when(F.col("status") == "FROZEN", F.col("balance"))).alias("frozen_max_balance"),
    )
)
