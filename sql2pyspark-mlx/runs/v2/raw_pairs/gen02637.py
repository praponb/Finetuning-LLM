from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "search")
    .select(
        "duration_sec",
        F.concat("page", F.lit(" - "), "event_type").alias("page_event_type"),
        F.concat("page", F.lit(" "), "event_type").alias("page_event_type_2"),
    )
)
