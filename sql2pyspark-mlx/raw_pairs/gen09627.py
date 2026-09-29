from pyspark.sql import functions as F

filtered = (
    df_web_events
    .filter((F.col("event_time") >= F.to_timestamp(F.lit("2023-04-30 17:00:00"))) & F.hour("event_time").between(6, 14))
    .select(
        "*",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
        (F.datediff(F.current_date(), "event_time") / 365.25).alias("years_since_event_time"),
    )
)

result = (
    filtered
    .alias("src")
    .join(df_sessions.alias("dim"), F.col("src.session_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["device", "event_type"], ["device"]],
        F.col("dim.device").alias("device"),
        F.col("src.event_type").alias("event_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("src.duration_sec")).alias("total_duration_sec"),
        F.round(F.avg(F.col("src.duration_sec_filled")), 2).alias("avg_duration_sec_filled"),
    )
    .orderBy(F.col("rows_total").asc_nulls_first())
)
