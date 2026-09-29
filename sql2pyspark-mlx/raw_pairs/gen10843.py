from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("account_id").isin([16, 18, 30, 40]))
    .groupBy("account_id")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_totals.alias("account_totals"), F.col("accounts.id") == F.col("account_totals.account_id"), "left")
    .filter(F.col("account_totals.stddev_amount") >= 54)
    .select(
        F.col("accounts.id"),
        F.col("accounts.account_type"),
        F.col("accounts.customer_id"),
        F.col("account_totals.stddev_amount"),
        F.col("account_totals.avg_amount"),
    )
    .orderBy(F.col("stddev_amount").desc(), F.col("id").desc())
)
