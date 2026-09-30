from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") >= "2025-05-01")
    .groupBy("status", "account_type")
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
    .filter(F.col("stddev_balance") >= 46)
    .orderBy("account_type", F.col("latest_opened_date").desc())
)
