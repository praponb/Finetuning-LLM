from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("a.status") == "FROZEN")
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"), F.col("c.is_active"))
    .agg(
        F.round(F.stddev(F.col("t.amount")), 2).alias("stddev_amount"),
        F.min(F.col("t.txn_time")).alias("min_txn_time"),
    )
    .filter(F.col("stddev_amount") >= 67)
    .orderBy(F.col("min_txn_time").desc())
)
