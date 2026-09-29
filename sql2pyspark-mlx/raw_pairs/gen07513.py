from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "page",
        F.upper("page").alias("page_upper"),
        F.datediff(F.current_date(), "event_time").alias("days_since_event_time"),
    )
)
