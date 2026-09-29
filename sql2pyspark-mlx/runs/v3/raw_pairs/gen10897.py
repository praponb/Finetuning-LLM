from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "duration_sec",
        "page",
        "event_time",
        F.concat_ws(",", "page", "event_type").alias("page_event_type_key"),
    )
)
