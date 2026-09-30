from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(~F.col("we.event_type").isin(["click", "add_to_cart", "search"]) & F.col("we.duration_sec").between(407, 451))
    .select(F.col("we.event_time"), F.col("s.country"), F.col("s.started_at"))
)
