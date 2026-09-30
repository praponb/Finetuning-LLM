from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") < "2023-07-01") & F.col("duration_sec").between(255, 325))
    .select(
        "id",
        "duration_sec",
        "session_id",
        F.ceil("duration_sec").alias("duration_sec_whole"),
    )
    .orderBy(F.col("duration_sec_whole").desc())
)
