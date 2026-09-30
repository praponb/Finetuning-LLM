from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("c.city") == "New York")
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.account_type"),
        F.col("c.city"),
        F.col("c.signup_date"),
        F.col("c.is_active"),
    )
    .orderBy(F.col("id").desc(), F.col("account_type").desc())
)
