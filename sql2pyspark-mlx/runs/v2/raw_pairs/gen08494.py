from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").isNull())
    .select(
        "event_time",
        "event_type",
        "session_id",
        F.when(F.col("page") == "/home", 1).otherwise(0).alias("is_home"),
        F.substring("event_type", 1, 2).alias("event_type_prefix"),
    )
    .orderBy("session_id", F.col("event_type").desc())
)
