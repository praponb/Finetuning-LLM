from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter(~F.col("status").isin(["CLOSED", "OPEN", "FROZEN"]) & F.col("balance").between(34000, 63000))
)

account_type_summary = (
    valid_accounts
    .groupBy("account_type", "status")
    .agg(F.round(F.stddev("balance"), 2).alias("stddev_balance"))
)

result = (
    account_type_summary
    .filter(F.col("stddev_balance") < 79)
    .select("account_type", "status", "stddev_balance")
    .orderBy("status")
)
