from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") != "LOAN")
    .groupBy("account_type", "status")
    .agg(F.round(F.stddev("balance"), 2).alias("stddev_balance"))
    .filter(F.col("stddev_balance") < 25)
    .select("account_type", "status", "stddev_balance")
    .orderBy("status")
)
