from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter((F.col("c.signup_date") > "2025-02-01") | (F.col("a.balance") > 17000))
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("a.status"),
        F.col("c.credit_limit"),
        F.col("c.age"),
        F.col("c.signup_date"),
    )
)
