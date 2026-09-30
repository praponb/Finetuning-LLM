from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter((F.col("we.event_type") == "page_view") & (F.col("we.event_time") >= "2024-12-01"))
    .select(
        F.col("s.browser"),
        F.col("s.customer_id"),
        F.col("s.device"),
        F.col("we.event_time"),
        F.col("we.page"),
        F.col("we.session_id"),
    )
    .orderBy(F.col("page").desc())
)
