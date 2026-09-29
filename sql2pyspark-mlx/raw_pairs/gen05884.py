from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2025-08-01", "2025-08-08"))
    .groupBy("account_type")
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.min("balance").alias("min_balance"),
    )
    .filter(F.col("stddev_balance") >= 61)
)
