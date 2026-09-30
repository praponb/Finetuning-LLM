from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.month(F.col("c.signup_date")).isin([4, 8, 12]))
    .select(
        F.col("a.customer_id"),
        F.col("c.is_active"),
        F.col("c.email"),
        F.col("c.signup_date"),
    )
)
