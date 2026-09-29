from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "event_time",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
        F.round("duration_sec", 1).alias("duration_sec_rounded"),
    )
)
