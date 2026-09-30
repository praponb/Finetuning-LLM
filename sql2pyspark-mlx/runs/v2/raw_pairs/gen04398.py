from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([7, 24, 30, 32]) & F.col("event_type").isin(["purchase", "search", "add_to_cart"]))
    .select(
        "id",
        "event_time",
        "page",
        "event_type",
        F.hour("event_time").alias("event_time_hour"),
    )
)
