from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "CHECKING")
    .groupBy("status", "account_type")
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.count("balance").alias("balance_count"),
    )
    .filter(F.col("stddev_balance") > 34)
    .select("status", "account_type", "stddev_balance", "balance_count")
)
