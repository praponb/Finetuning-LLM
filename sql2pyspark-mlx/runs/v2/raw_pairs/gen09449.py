from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter((F.col("c.credit_limit") >= 7300) | F.col("c.email").like("%@startup.io"))
    .select(
        F.col("c.is_active"),
        F.col("c.segment"),
        F.col("c.country"),
        F.col("a.customer_id"),
    )
    .orderBy(F.col("customer_id").desc(), "segment")
)
