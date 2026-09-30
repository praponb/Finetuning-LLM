from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter(F.col("we.event_type").isin(["add_to_cart", "click", "page_view"]))
    .select(
        F.col("s.customer_id"),
        F.col("s.browser"),
        F.col("s.started_at"),
        F.col("we.event_type"),
    )
)
