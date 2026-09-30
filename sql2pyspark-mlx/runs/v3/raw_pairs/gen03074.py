from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "OPEN")
    .groupBy("account_type", "status")
    .agg(F.round(F.stddev("balance"), 2).alias("stddev_balance"))
    .orderBy("stddev_balance", "account_type", "status")
    .limit(20)
)
