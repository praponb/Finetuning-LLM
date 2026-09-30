from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter((F.col("c.city") == "New York") & F.col("c.email").isNotNull())
    .select(
        F.col("a.customer_id"),
        F.col("a.account_type"),
        F.col("a.balance"),
        F.col("c.segment"),
    )
)
