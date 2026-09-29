from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 36)
    .select(
        "id",
        "event_type",
        "event_time",
        "page",
        F.initcap(F.trim("event_type")).alias("event_type_title"),
    )
    .orderBy(F.col("id").desc())
)
