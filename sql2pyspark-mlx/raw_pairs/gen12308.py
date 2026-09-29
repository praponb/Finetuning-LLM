from pyspark.sql import functions as F

valid_accounts = df_accounts.filter(F.col("customer_id").isNotNull())

status_summary = (
    valid_accounts
    .groupBy("status")
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
)

result = status_summary.filter(F.col("stddev_balance") > 40)
