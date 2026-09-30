from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter(~F.col("we.event_type").isin(["search", "purchase"]) | F.col("s.browser").isin(["Firefox", "Chrome", "Edge"]))
    .select(
        F.col("we.id"),
        F.col("we.session_id"),
        F.col("we.duration_sec"),
        F.col("we.page"),
        F.col("s.country"),
    )
    .orderBy("duration_sec")
)
