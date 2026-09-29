from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter((F.col("event_time") > F.to_date(F.lit("2023-07-23"))) & F.col("session_id").isin([3, 30]))
    .withColumn("event_type_length", F.length("event_type"))
)

result = (
    cleaned
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "inner")
    .groupBy(
        F.col("p.browser").alias("browser"),
        F.col("p.country").alias("country"),
        F.col("src.event_type").alias("event_type"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("src.event_type_length")).alias("max_event_type_length"),
        F.max(F.col("src.duration_sec")).alias("max_duration_sec"),
    )
    .orderBy(F.col("cnt").desc_nulls_last())
)
