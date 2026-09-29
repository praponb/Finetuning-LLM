from pyspark.sql import functions as F

recent_web_events = (
    df_web_events
    .filter((F.col("session_id") == 39) & F.hour("event_time").between(9, 15))
)

summary = (
    recent_web_events
    .groupBy("page")
    .agg(
        F.sum(F.when(F.col("page") == "/search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
    )
)

result = (
    summary
    .select("page", "search_duration_sec")
    .orderBy("page")
)
