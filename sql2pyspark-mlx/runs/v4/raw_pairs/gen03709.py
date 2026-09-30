from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter((F.col("s.customer_id") == 22) & (F.col("c.credit_limit") > 11300))
    .select(
        F.col("s.started_at"),
        F.col("s.country"),
        F.col("c.signup_date"),
        F.col("c.age"),
    )
    .orderBy("age")
)
