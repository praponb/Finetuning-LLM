from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") == 438) | (F.col("page") == "/cart"))
    .select(
        "id",
        "event_time",
        "session_id",
        F.to_date("event_time").alias("event_time_date"),
    )
)
