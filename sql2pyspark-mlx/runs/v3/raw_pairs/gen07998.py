from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_time",
        "page",
        F.trim("event_type").alias("clean_event_type"),
        F.substring("page", 1, 2).alias("page_prefix"),
    )
    .orderBy("page")
)
