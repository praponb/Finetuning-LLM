from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter(F.col("we.duration_sec").between(191, 308) & F.col("s.country").isin(["TH", "SG", "US"]))
    .select(
        F.col("we.duration_sec"),
        F.col("we.event_type"),
        F.col("s.country"),
        F.col("s.customer_id"),
        F.col("s.device"),
    )
)
