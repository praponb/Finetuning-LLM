from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.status"),
        F.col("a.customer_id"),
        F.col("c.is_active"),
    )
    .orderBy("customer_id", F.col("id").desc())
    .limit(3)
)
