from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("c.signup_date") < "2025-08-01") & F.month(F.col("a.opened_date")).isin([1, 2, 6]))
    .select(
        F.col("a.customer_id"),
        F.col("a.opened_date"),
        F.col("c.credit_limit"),
        F.col("c.email"),
    )
)
