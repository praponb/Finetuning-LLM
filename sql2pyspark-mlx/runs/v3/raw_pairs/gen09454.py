from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .select(
        F.col("c.age"),
        F.col("c.is_active"),
        F.col("a.account_type"),
        F.col("a.balance"),
    )
)
