from pyspark.sql import functions as F

by_dimension = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["device", "event_type"], ["device"]],
        F.col("p.device").alias("device"),
        F.col("a.event_type").alias("event_type"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("a.duration_sec")).alias("min_duration_sec"),
    )
)

result = (
    by_dimension
    .select("device", "event_type", "min_duration_sec")
    .orderBy(F.col("device").asc_nulls_last())
)
