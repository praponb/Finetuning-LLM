from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.to_date("event_time") < "2024-06-24") & F.col("duration_sec").between(225, 461))
    .select(
        "id",
        "duration_sec",
        "page",
        "session_id",
        F.lower("event_type").alias("event_type_lower"),
        F.floor("duration_sec").alias("duration_sec_whole"),
    )
    .orderBy(F.col("event_type_lower").desc(), F.col("session_id").desc())
)
