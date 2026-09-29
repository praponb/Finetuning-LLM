from pyspark.sql import functions as F

filtered_accounts = (
    df_accounts
    .filter((F.col("balance") > 29000) | F.col("status").isin(["FROZEN", "CLOSED", "OPEN"]))
)

summary = (
    filtered_accounts
    .groupBy("account_type")
    .agg(F.round(F.stddev("balance"), 2).alias("stddev_balance"))
)

result = (
    summary
    .filter(F.col("stddev_balance") > 45)
    .select("account_type", "stddev_balance")
)
