from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "right")
    .groupBy(F.quarter(F.col("c.signup_date")).alias("signup_quarter"), F.col("c.city"))
    .agg(
        F.min(F.col("t.amount")).alias("min_amount"),
        F.min(F.col("t.txn_time")).alias("min_txn_time"),
    )
    .orderBy(F.col("city").desc())
)
