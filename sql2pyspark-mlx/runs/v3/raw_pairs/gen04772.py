from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.name").like("Ol%"))
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("c.credit_limit"),
        F.col("c.name"),
        F.col("c.email"),
    )
    .orderBy("email", "started_at")
)
