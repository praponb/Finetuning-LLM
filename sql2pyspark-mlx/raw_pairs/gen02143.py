from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("we.event_time"),
        F.col("s.browser"),
        F.col("s.country"),
        F.col("c.city"),
        F.quarter(F.col("s.started_at")).alias("started_at_quarter"),
    )
    .orderBy("city")
)
