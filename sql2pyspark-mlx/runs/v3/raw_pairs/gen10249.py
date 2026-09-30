from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.browser").isNotNull())
    .select(
        F.col("we.id"),
        F.col("we.page"),
        F.col("we.duration_sec"),
        F.col("s.started_at"),
        F.col("c.signup_date"),
        F.add_months(F.col("c.signup_date"), 6).alias("signup_date_plus_months"),
    )
    .orderBy(F.col("id").desc())
)
