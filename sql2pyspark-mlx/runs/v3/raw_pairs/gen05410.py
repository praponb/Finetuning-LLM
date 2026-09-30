from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.event_type").isin(["add_to_cart", "page_view"]))
    .groupBy(F.date_trunc("MONTH", F.col("s.started_at")).alias("started_month"))
    .agg(F.min(F.col("we.event_time")).alias("min_event_time"))
    .orderBy(F.col("started_month").desc(), "min_event_time")
)
