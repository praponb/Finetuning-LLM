from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter(F.col("s.device").isin(["desktop", "mobile"]))
    .groupBy(F.col("s.id"), F.col("s.country"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.countDistinct(F.col("we.duration_sec")).alias("unique_pages"),
    )
    .filter(F.col("unique_pages") >= 1)
    .orderBy(F.col("unique_pages").desc())
)
