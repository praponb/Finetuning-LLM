from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .select(
        F.col("a.opened_date"),
        F.col("a.account_type"),
        F.col("a.balance"),
        F.col("c.segment"),
        F.col("c.name"),
        F.col("c.city"),
    )
)
