from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(((F.col("event_time") < "2023-09-01") & (F.col("session_id") == 7)) | (F.col("event_time") >= "2023-12-01"))
    .select(
        "id",
        "event_type",
        F.round("duration_sec", 1).alias("duration_sec_rounded"),
    )
    .orderBy(F.col("duration_sec_rounded").desc())
)
