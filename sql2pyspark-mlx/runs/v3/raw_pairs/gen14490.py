from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "outer")
    .filter((F.col("c.signup_date") > "2024-08-19") | F.col("c.city").isNotNull())
    .select(
        F.col("t.id"),
        F.col("t.amount"),
        F.col("t.merchant"),
        F.col("a.status"),
        F.col("a.balance"),
        F.col("c.email"),
        F.col("c.credit_limit"),
    )
    .orderBy("credit_limit")
)
