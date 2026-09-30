from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter(F.col("we.event_type").isin(["search", "purchase", "add_to_cart"]))
    .select(
        F.col("s.device"),
        F.col("s.customer_id"),
        F.col("s.browser"),
        F.col("we.event_type"),
        F.col("we.duration_sec"),
    )
    .orderBy("customer_id", "event_type")
)
