from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(243, 266) | (F.col("duration_sec") != 265))
    .select(
        "id",
        "event_type",
        "session_id",
        "duration_sec",
        F.lower("event_type").alias("event_type_lower"),
    )
)
