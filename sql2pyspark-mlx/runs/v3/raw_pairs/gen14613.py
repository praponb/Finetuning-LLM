from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_time",
        F.concat_ws(",", "event_type", "page").alias("event_type_page_key"),
        F.date_trunc("year", "event_time").alias("event_time_period"),
    )
    .orderBy("event_time", "id")
    .limit(5)
)
