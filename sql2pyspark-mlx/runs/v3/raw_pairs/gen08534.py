from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([5, 14, 16, 26]) & ~F.col("event_type").isin(["page_view", "click"]) & (F.col("duration_sec") != 145))
    .select(
        "id",
        "event_type",
        "duration_sec",
        F.upper("event_type").alias("event_type_upper"),
    )
)
