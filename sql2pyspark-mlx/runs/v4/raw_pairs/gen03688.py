from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "outer")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "right")
    .groupBy(
        F.when(F.year(F.col("c.signup_date")) < 2024, "before_2024").when(F.year(F.col("c.signup_date")) == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
        F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"),
    )
    .agg(
        F.min(F.col("t.txn_time")).alias("first_txn_time"),
        F.round(F.stddev(F.col("t.amount")), 2).alias("stddev_amount"),
    )
    .orderBy("signup_cohort", "signup_month")
    .limit(50)
)
