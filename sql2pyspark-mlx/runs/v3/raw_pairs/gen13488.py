from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id") == 29) & F.col("page").isNull())
    .groupBy(F.hour("event_time").alias("event_hour"))
    .agg(
        F.sum(F.when(F.col("event_type") == "search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
        F.count("*").alias("row_count"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
    .orderBy(F.col("min_duration_sec").desc())
)
