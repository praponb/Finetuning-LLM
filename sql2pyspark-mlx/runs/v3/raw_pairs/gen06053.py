from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(((F.col("duration_sec") < 242) & F.col("session_id").isin([6, 31, 39])) | (F.col("event_type") != "search"))
    .select(
        "id",
        "event_type",
        "event_time",
        "page",
        F.round("duration_sec", 2).alias("duration_sec_rounded"),
        F.round((F.col("duration_sec") * 1.15), 2).alias("duration_sec_adjusted"),
    )
)
