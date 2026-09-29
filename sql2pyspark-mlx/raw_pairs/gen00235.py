from pyspark.sql import functions as F

result = (
    df_web_events
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "inner")
    .groupingSets(
        [["browser", "event_type"], ["browser"], []],
        F.col("s.browser").alias("browser"),
        F.col("src.event_type").alias("event_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("src.duration_sec")).alias("max_duration_sec"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("rows_total").desc_nulls_first())
)
