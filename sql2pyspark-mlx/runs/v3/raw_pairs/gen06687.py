from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "right")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.month(F.col("c.signup_date")).isin([8, 11, 12]))
    .select(
        F.col("we.id"),
        F.col("we.page"),
        F.col("s.started_at"),
        F.col("s.browser"),
        F.col("c.name"),
        F.col("c.email"),
    )
)
