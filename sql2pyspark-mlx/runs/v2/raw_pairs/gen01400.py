from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("c.is_active"))
    .select(
        F.col("s.id"),
        F.col("s.started_at"),
        F.col("s.country"),
        F.col("c.age"),
        F.col("c.segment"),
        F.col("c.signup_date"),
    )
    .orderBy(F.col("age").desc(), "started_at")
)
