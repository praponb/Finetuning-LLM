from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .select(
        F.col("c.credit_limit"),
        F.col("c.city"),
        F.col("c.age"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("s.started_at"),
    )
    .orderBy(F.col("city").desc())
)
