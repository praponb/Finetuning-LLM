from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("we.event_time"),
        F.col("s.browser"),
        F.col("c.segment"),
        F.col("c.is_active"),
        (F.col("c.age") + F.col("c.credit_limit")).alias("age_plus_credit_limit"),
    )
)
