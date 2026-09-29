from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "right")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("we.session_id"),
        F.col("s.customer_id"),
        F.col("c.age"),
        F.col("c.email"),
        F.date_format(F.col("we.event_time"), "yyyy-MM").alias("event_time_label"),
    )
    .orderBy(F.col("age").desc())
)
