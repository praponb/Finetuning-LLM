from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.country").isin(["US", "DE", "UK"]) & F.col("a.customer_id").isNotNull())
    .select(
        F.col("a.balance"),
        F.col("a.customer_id"),
        F.col("c.name"),
        F.col("c.country"),
    )
    .orderBy(F.col("customer_id").desc())
)
