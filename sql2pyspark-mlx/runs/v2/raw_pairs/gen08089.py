from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter((F.col("c.credit_limit") <= 10500) | F.month(F.col("c.signup_date")).isin([1, 9, 12]))
    .select(
        F.col("a.customer_id"),
        F.col("a.status"),
        F.col("a.opened_date"),
        F.col("c.signup_date"),
        F.col("c.is_active"),
        F.col("c.city"),
        F.upper(F.col("c.email")).alias("email_upper"),
    )
    .orderBy("signup_date", "email_upper")
)
