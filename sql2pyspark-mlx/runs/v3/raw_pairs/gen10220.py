from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status", "account_type")
    .agg(F.round(F.stddev("balance"), 2).alias("stddev_balance"))
    .filter(F.col("stddev_balance") < 57)
    .select("status", "account_type", "stddev_balance")
)
