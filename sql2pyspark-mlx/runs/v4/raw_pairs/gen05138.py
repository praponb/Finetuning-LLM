from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 32)
    .groupBy("customer_id", "account_type")
    .agg(F.round(F.stddev("balance"), 2).alias("stddev_balance"))
    .filter(F.col("stddev_balance") < 70)
    .select("customer_id", "account_type", "stddev_balance")
)
