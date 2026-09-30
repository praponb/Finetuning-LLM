from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") < 440) & (F.col("session_id").isin([2, 14, 34, 40]) | (F.col("event_type") == "search")))
    .select(
        "id",
        "page",
        "event_type",
        "duration_sec",
        F.concat_ws("-", "page", "event_type").alias("page_event_type_key"),
    )
)
