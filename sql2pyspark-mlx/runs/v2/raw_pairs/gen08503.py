from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("a.status") == "FROZEN") & F.col("c.name").like("%Brown%"))
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.balance"),
        F.col("a.customer_id"),
        F.col("c.credit_limit"),
        F.col("c.age"),
    )
)
