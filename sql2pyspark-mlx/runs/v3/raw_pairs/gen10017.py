from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(33000, 40000))
    .groupBy("status")
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.count("balance").alias("balance_count"),
    )
)
