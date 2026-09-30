from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("s.device") == "desktop")
    .select(
        F.col("we.id"),
        F.col("we.session_id"),
        F.col("s.browser"),
        F.col("c.segment"),
        F.col("c.signup_date"),
    )
)
