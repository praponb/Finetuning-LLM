from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 18)
    .select(
        "event_time",
        "event_type",
        F.concat_ws("-", "page", "event_type").alias("page_event_type_key"),
        F.lower("page").alias("page_lower"),
    )
)
