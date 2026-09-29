from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "outer")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("we.session_id"),
        F.col("we.page"),
        F.col("s.customer_id"),
        F.col("s.browser"),
        F.col("c.signup_date"),
    )
)
