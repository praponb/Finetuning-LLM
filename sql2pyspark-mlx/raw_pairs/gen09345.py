from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") != 326) & ((F.col("duration_sec") == 307) | F.col("event_type").isin(["click", "page_view", "purchase"])))
    .select(
        "event_type",
        F.concat_ws("-", "event_type", "page").alias("event_type_page_key"),
        F.trim("event_type").alias("clean_event_type"),
    )
    .orderBy("event_type")
)
