from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") >= "2023-10-13")
    .select(
        "id",
        "event_type",
        "session_id",
        F.concat("page", F.lit(" "), "event_type").alias("page_event_type"),
    )
)
