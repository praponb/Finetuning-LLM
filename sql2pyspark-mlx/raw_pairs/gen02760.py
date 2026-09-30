from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "duration_sec",
        "event_time",
        "page",
        F.year("event_time").alias("event_time_year"),
        F.coalesce("page", F.lit("N/A")).alias("page_filled"),
    )
)
