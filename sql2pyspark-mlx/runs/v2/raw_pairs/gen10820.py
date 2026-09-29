from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([5, 6, 13]))
    .select(
        "event_type",
        "duration_sec",
        "session_id",
        F.ceil("duration_sec").alias("duration_sec_whole"),
    )
)
