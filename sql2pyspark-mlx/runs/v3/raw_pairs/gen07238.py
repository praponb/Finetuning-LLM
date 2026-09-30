from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "CREDIT")
    .groupBy("account_type")
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.min("balance").alias("min_balance"),
    )
)
