from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter((F.col("we.event_type") != "page_view") & F.col("s.device").isin(["tablet", "mobile", "desktop"]))
    .select(
        F.col("s.device"),
        F.col("s.browser"),
        F.col("s.country"),
        F.col("we.event_type"),
    )
)
