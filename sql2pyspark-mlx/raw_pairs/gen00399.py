from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter((F.col("event_time") > F.to_timestamp(F.lit("2023-03-19 08:00:00"))) & F.col("page").isNotNull())
    .withColumn("event_time_day", F.col("event_time").cast("date"))
    .withColumn("page_clean", F.coalesce("page", F.lit("NONE")))
)

result = (
    cleaned
    .alias("e")
    .join(df_sessions.alias("p"), F.col("e.session_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["browser", "event_type"], ["browser"], []],
        F.col("p.browser").alias("browser"),
        F.col("e.event_type").alias("event_type"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.duration_sec")).alias("max_duration_sec"),
    )
)
