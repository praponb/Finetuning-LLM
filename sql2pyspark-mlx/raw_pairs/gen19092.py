from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "left")
    .select(
        F.col("a.id"),
        F.col("a.customer_id"),
        F.col("c.city"),
        F.col("c.is_active"),
        F.col("c.segment"),
    )
)
