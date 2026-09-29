from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(~F.col("we.event_type").isin(["add_to_cart", "page_view"]))
    .groupBy(F.col("s.browser"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.max(F.col("we.event_time")).alias("last_event_time"),
    )
    .orderBy(F.col("web_event_count").desc(), "last_event_time", "browser")
    .limit(3)
)
