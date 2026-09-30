from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.is_active"))
    .select(
        F.col("c.email"),
        F.col("a.status"),
        F.col("a.balance"),
        F.col("a.opened_date"),
    )
    .orderBy(F.col("status").desc())
)
