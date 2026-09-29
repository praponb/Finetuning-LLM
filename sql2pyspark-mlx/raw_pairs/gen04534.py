from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "left")
    .select(
        F.col("a.opened_date"),
        F.col("a.account_type"),
        F.col("a.customer_id"),
        F.col("c.email"),
        F.col("c.age"),
        F.col("c.signup_date"),
    )
    .orderBy(F.col("email").desc())
)
