from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") == 406)
    .select(
        "session_id",
        "page",
        F.hour("event_time").alias("event_time_hour"),
        F.trim("page").alias("clean_page"),
    )
)
