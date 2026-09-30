from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.to_date("event_time") >= "2023-05-10") | (F.col("event_type") == "search"))
    .select(
        "session_id",
        "event_type",
        "page",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
    )
)
