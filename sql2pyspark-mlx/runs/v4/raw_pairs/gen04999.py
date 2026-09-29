from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(~((F.col("s.customer_id") == 15) & F.col("s.browser").isin(["Chrome", "Firefox"])))
    .select(
        F.col("we.id"),
        F.col("we.duration_sec"),
        F.col("we.event_time"),
        F.col("s.device"),
    )
    .orderBy(F.col("id").desc())
)
