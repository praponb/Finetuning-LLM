from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([4, 16, 39, 40]) & ((F.col("event_type") == "search") | F.col("session_id").isin([8, 18, 29])))
    .select(
        "id",
        "duration_sec",
        "session_id",
        F.upper("event_type").alias("event_type_upper"),
    )
    .orderBy("id", F.col("session_id").desc())
)
