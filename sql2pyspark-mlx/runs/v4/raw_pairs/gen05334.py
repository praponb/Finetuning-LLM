from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.month(F.col("c.signup_date")).isin([6, 7, 9]) & (F.col("c.signup_date") < "2025-09-01"))
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.balance"),
        F.col("c.age"),
        F.col("c.is_active"),
        F.col("c.credit_limit"),
    )
)
