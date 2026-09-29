from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page", "event_type")
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.countDistinct("duration_sec").alias("unique_sessions"),
    )
    .filter(F.col("unique_sessions") >= 8)
    .select("page", "event_type", "duration_sec_count", "unique_sessions")
    .orderBy(F.col("page").desc())
)
