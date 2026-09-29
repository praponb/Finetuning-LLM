from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("c.credit_limit") >= 11300) & F.col("s.status").isin(["CANCELLED", "PAUSED"]))
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("s.status"),
        F.col("c.segment"),
        F.col("c.email"),
    )
)
