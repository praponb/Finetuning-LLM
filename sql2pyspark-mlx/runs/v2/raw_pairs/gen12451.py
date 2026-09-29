from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("c.signup_date").between("2023-11-01", "2024-09-01") & F.col("c.email").like("%@yahoo.com"))
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.status"),
        F.col("c.is_active"),
        F.datediff(F.current_date(), F.col("c.signup_date")).alias("days_since_signup_date"),
    )
)
