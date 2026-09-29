from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~(~F.col("page").isin(["/search", "/home", "/checkout"]) & (F.col("duration_sec") != 277)))
    .select(
        "event_time",
        "session_id",
        F.date_format("event_time", "MMM yyyy").alias("event_time_label"),
    )
)
