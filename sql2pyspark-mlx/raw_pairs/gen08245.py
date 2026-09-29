from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter(F.col("we.session_id").isin([20, 27, 30, 34]) & F.col("we.event_type").isin(["click", "page_view"]))
    .select(
        F.col("we.id"),
        F.col("we.page"),
        F.col("we.duration_sec"),
        F.col("we.session_id"),
        F.col("s.country"),
        F.col("s.customer_id"),
        F.col("s.browser"),
    )
)
