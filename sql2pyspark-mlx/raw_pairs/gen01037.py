from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter(F.col("page") == "/search")
    .withColumn("page_event_type_key", F.concat_ws("-", "page", "event_type"))
    .withColumn("page_title", F.initcap(F.trim("page")))
    .withColumn("resolved_page", F.coalesce("page", F.lit("UNKNOWN")))
)

result = (
    cleaned
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.country").alias("country"), F.col("e.page").alias("page"))
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("e.duration_sec")).alias("max_duration_sec"),
    )
)
