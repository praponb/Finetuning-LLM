from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "event_time",
        F.concat_ws("/", "page", "event_type").alias("page_event_type_key"),
        F.hour("event_time").alias("event_time_hour"),
    )
    .orderBy("event_time")
)
