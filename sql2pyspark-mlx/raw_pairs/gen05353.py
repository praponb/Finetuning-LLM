from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "left")
    .select(
        F.col("t.account_id"),
        F.col("t.merchant"),
        F.col("a.account_type"),
        F.col("a.balance"),
        F.col("c.country"),
        F.col("c.age"),
        F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_date_period"),
    )
    .orderBy(F.col("account_id").desc())
)
