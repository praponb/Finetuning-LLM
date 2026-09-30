from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("s.browser").isin(["Edge", "Chrome", "Firefox"]))
    .select(
        F.col("s.started_at"),
        F.col("s.browser"),
        F.col("s.country"),
        F.col("c.signup_date"),
        F.col("c.is_active"),
        F.col("c.country").alias("customer_country"),
    )
    .orderBy(F.col("country").desc(), "started_at")
)
