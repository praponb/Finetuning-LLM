from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.min("balance").alias("min_balance"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
    .filter(F.col("stddev_balance") >= 40)
    .select("account_type", "min_balance", "stddev_balance")
    .orderBy(F.col("min_balance").desc(), "account_type")
    .limit(5)
)
