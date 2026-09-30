from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("row_count").desc(), F.col("avg_duration_sec").desc(), "event_type")
    .limit(5)
)
