from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(190, 406))
    .select(
        "duration_sec",
        "session_id",
        "event_type",
        F.upper("page").alias("page_upper"),
        F.col("event_time").cast("date").alias("event_time_day"),
    )
    .orderBy(F.col("session_id").desc(), F.col("page_upper").desc())
)
