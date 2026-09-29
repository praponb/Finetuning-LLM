from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter(F.col("page") != "/checkout")
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        F.hour("event_time").alias("event_time_hour"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_sessions.alias("dim"), F.col("s.session_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["country", "event_type"], ["country"]],
        F.col("dim.country").alias("country"),
        F.col("s.event_type").alias("event_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("s.duration_sec")).alias("avg_duration_sec"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("country").asc_nulls_first())
)
