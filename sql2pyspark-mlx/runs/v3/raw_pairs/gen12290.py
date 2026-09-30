from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.count("*").alias("total_count"),
        F.avg("balance").alias("avg_balance"),
    )
    .orderBy("stddev_balance")
)
