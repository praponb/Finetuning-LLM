from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") > 223) | (F.to_date("event_time") < "2024-08-01"))
    .select(
        "session_id",
        "event_type",
        F.concat("page", F.lit(", "), "event_type").alias("page_event_type"),
    )
)
