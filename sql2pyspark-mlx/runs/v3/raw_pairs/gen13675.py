from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.started_at") >= "2025-07-22")
    .select(
        F.col("s.started_at"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("c.segment"),
        F.col("c.credit_limit"),
        F.col("c.country"),
    )
)
