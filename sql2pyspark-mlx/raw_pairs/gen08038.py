from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "event_time",
        "page",
        "event_type",
        F.round("duration_sec", 1).alias("duration_sec_rounded"),
    )
)
