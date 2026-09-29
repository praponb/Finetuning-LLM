from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.to_date("event_time") >= "2024-08-02") & F.col("duration_sec").between(236, 456))
    .select("id", "session_id", "page", "duration_sec")
    .orderBy(F.col("session_id").desc())
)
