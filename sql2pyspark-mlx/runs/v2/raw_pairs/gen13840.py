from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(F.round(F.stddev("balance"), 2).alias("stddev_balance"))
    .filter(F.col("stddev_balance") < 45)
    .select("status", "stddev_balance")
    .orderBy(F.col("stddev_balance").desc())
)
