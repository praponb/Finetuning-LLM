from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([12, 23, 28, 30]))
    .select(
        "id",
        "event_time",
        "duration_sec",
        "page",
        F.initcap(F.trim("event_type")).alias("event_type_title"),
        F.lower("page").alias("page_lower"),
    )
)
