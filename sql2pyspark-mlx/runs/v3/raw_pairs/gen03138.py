from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id").isin([1, 15, 18, 36]) & (F.col("event_type") == "click")) | F.col("session_id").isin([12, 27, 34, 37]))
    .select(
        "id",
        "session_id",
        "duration_sec",
        "event_time",
        F.date_trunc("YEAR", "event_time").alias("event_time_period"),
    )
    .orderBy("event_time_period")
)
