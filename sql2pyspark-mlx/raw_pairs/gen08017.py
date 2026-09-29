from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter(F.col("event_time") > F.to_timestamp(F.lit("2023-04-30 20:00:00")))
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.to_date("event_time").alias("event_time_date"),
        F.length("event_type").alias("event_type_length"),
    )
)

result = (
    cleaned
    .select("id", "page")
    .intersectAll(
        df_web_events
        .filter(F.hour("event_time").between(10, 17))
        .select("id", "page")
    )
)
