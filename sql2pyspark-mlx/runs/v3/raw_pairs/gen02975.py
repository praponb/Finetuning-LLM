from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.browser").isin(["Chrome", "Safari"]))
    .select(
        F.col("we.session_id"),
        F.col("s.started_at"),
        F.col("s.device"),
        F.col("s.customer_id"),
    )
)
