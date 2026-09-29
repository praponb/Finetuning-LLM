from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.quarter("opened_date").alias("opened_quarter"), "account_type")
    .agg(
        F.min("balance").alias("min_balance"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.count("balance").alias("balance_count"),
    )
    .filter(F.col("min_balance") > 44000)
    .orderBy("account_type", "opened_quarter")
    .limit(5)
)
