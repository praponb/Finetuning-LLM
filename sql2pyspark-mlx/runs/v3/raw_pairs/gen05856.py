from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "page",
        "event_time",
        F.upper("event_type").alias("event_type_upper"),
        F.concat_ws("-", "page", "event_type").alias("page_event_type_key"),
    )
    .orderBy(F.col("id").desc())
)
