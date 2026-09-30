from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 23)
    .groupBy("status")
    .agg(
        F.min("balance").alias("min_balance"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
    .filter(F.col("min_balance") < 74000)
    .select("status", "min_balance", "stddev_balance")
    .orderBy(F.col("status").desc())
    .limit(5)
)
