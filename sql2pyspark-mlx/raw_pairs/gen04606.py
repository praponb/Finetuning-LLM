from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("a.account_type"),
        F.col("a.status"),
        F.col("c.age"),
        F.col("c.signup_date"),
        F.col("c.country"),
    )
    .orderBy("signup_date")
)
