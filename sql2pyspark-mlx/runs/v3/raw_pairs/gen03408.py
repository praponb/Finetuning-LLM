from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.customer_id"),
        F.col("a.account_type"),
        F.col("c.name"),
        F.col("c.city"),
    )
    .orderBy(F.col("account_type").desc())
)
