from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "CHECKING"]))
    .agg(
        F.countDistinct("balance").alias("unique_customers"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
)
