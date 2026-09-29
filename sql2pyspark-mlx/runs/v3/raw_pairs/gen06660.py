from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id") == 15) & (F.col("duration_sec") > 366))
    .select(
        "id",
        "event_time",
        "session_id",
        F.concat_ws("/", "event_type", "page").alias("event_type_page_key"),
    )
)
