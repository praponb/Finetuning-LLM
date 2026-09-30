from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(174, 418) & F.col("event_type").isin(["click", "search", "add_to_cart"]))
    .select(
        "id",
        "event_time",
        F.hour("event_time").alias("event_time_hour"),
        F.col("event_time").cast("date").alias("event_time_day"),
    )
)
