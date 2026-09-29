from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") == "/checkout") & (F.col("duration_sec") > 170))
    .select(
        "id",
        "page",
        "session_id",
        F.date_format("event_time", "yyyy-MM-dd").alias("event_time_label"),
    )
)
