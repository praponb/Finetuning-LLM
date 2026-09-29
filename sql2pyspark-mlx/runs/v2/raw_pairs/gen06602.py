from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "outer")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.age").between(43, 52) & (F.col("c.country") != "TH"))
    .select(
        F.col("we.session_id"),
        F.col("s.customer_id"),
        F.col("c.is_active"),
        F.col("c.signup_date"),
    )
)
