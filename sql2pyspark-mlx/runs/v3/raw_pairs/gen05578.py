from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("a.opened_date"),
        F.col("c.segment"),
        F.col("c.name"),
    )
    .orderBy(F.col("status").desc())
)
