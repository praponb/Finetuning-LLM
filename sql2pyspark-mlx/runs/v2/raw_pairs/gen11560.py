from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") > 398) & F.col("session_id").isin([2, 16, 20, 22]))
    .select(
        "id",
        "session_id",
        "event_time",
        F.round("duration_sec", 0).alias("duration_sec_rounded"),
    )
    .orderBy("id")
    .limit(20)
)
