from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "right")
    .filter(F.col("c.name").like("A%"))
    .select(
        F.col("a.id"),
        F.col("a.opened_date"),
        F.col("a.balance"),
        F.col("c.email"),
        F.col("c.credit_limit"),
        F.col("c.name"),
    )
)
