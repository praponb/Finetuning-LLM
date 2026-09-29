from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "right")
    .select(
        F.col("a.customer_id"),
        F.col("c.is_active"),
        F.col("c.credit_limit"),
        F.col("c.signup_date"),
    )
)
