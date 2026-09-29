from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.customer_id").isin([1, 14, 32, 36]))
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("s.browser"),
        F.col("c.credit_limit"),
        F.col("c.city"),
    )
    .orderBy("browser")
)
