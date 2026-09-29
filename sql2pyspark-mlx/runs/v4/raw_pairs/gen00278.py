from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter(~F.col("s.browser").isin(["Edge", "Safari"]))
    .select(
        F.col("we.event_type"),
        F.col("we.session_id"),
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("s.country"),
        F.datediff(F.current_date(), F.col("we.event_time")).alias("days_since_event_time"),
    )
)
