from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec",
        F.initcap(F.trim("page")).alias("page_title"),
    )
    .orderBy(F.col("page_title").desc())
)
