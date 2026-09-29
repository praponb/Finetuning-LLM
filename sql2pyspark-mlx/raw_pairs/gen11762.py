from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter((F.col("s.browser") == "Safari") | F.col("we.session_id").isin([31, 36, 37, 39]))
    .select(
        F.col("s.customer_id"),
        F.col("s.country"),
        F.col("we.event_time"),
        F.col("we.event_type"),
        F.col("we.page"),
    )
    .orderBy("event_time")
)
