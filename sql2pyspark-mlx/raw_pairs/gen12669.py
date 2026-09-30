from pyspark.sql import functions as F

dim_totals = (
    df_web_events
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "left")
    .groupingSets(
        [["device", "event_type"], ["device"], []],
        F.col("s.device").alias("device"),
        F.col("src.event_type").alias("event_type"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("src.duration_sec")), 2).alias("avg_duration_sec"),
    )
)

result = (
    dim_totals
    .select(
        "device",
        "event_type",
        "cnt",
        "avg_duration_sec",
        F.when(F.col("device").rlike("^(mobile|tablet)$"), "GROUP_A").when(F.col("device").rlike("^(desktop)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("device_zone"),
    )
    .orderBy(F.col("avg_duration_sec").desc(), F.col("device_zone").desc())
)
