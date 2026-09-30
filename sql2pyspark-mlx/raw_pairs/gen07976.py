from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "right")
    .select(
        F.col("we.event_time"),
        F.col("we.event_type"),
        F.col("we.page"),
        F.col("s.browser"),
        F.initcap(F.trim(F.col("we.page"))).alias("page_title"),
    )
    .orderBy("page")
)
