from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .select(
        F.col("we.id"),
        F.col("we.event_type"),
        F.col("s.customer_id"),
        F.col("s.country"),
        F.col("s.browser"),
    )
    .orderBy(F.col("customer_id").desc(), "id")
    .limit(3)
)
