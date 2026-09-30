from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([35, 36, 40]))
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        F.initcap(F.trim("page")).alias("page_title"),
    )
)
