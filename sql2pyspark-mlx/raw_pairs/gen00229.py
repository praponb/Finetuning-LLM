from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([5, 27]))
    .select(
        "id",
        "event_type",
        "page",
        "session_id",
        F.trim("event_type").alias("clean_event_type"),
        F.lower("page").alias("page_lower"),
    )
)
