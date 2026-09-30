from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("max_duration_sec").desc())
)
