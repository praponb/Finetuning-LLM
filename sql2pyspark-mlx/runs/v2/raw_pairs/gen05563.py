from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.country") == "SG")
    .select(
        F.col("we.event_type"),
        F.col("s.started_at"),
        F.col("c.name"),
        F.col("c.age"),
        (F.col("c.age") * F.col("c.credit_limit")).alias("age_times_credit_limit"),
    )
)
