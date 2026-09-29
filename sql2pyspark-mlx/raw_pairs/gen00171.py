from pyspark.sql import functions as F

valid = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-06-23 12:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.lower("page").alias("page_lower"),
        F.date_trunc("week", "event_time").alias("event_time_period"),
        F.coalesce("page", F.lit("NONE")).alias("page_clean"),
    )
)

result = (
    valid
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "inner")
    .select(
        "src.id",
        "src.session_id",
        "src.page",
        "src.event_time",
        "src.page_lower",
        "src.event_time_period",
        "src.page_clean",
        F.col("s.browser"),
    )
)
