from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "right")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .groupBy(
        F.date_trunc("MONTH", F.col("customers.signup_date")).alias("signup_month"),
    )
    .agg(
        F.sum(F.col("transactions.amount")).alias("total_amount"),
        F.round(F.stddev(F.col("transactions.amount")), 2).alias("stddev_amount"),
    )
    .orderBy(F.col("total_amount").desc())
)
