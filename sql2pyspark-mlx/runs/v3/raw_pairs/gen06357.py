from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.browser") == "Firefox")
    .select(
        F.col("we.id"),
        F.col("we.session_id"),
        F.col("s.started_at"),
        F.col("s.country"),
        F.col("c.segment"),
        F.col("c.email"),
    )
    .orderBy("email")
)
