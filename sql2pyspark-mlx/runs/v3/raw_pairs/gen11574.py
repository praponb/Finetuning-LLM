from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
    .filter(F.col("max_duration_sec") >= 480)
    .select("event_type", "avg_duration_sec", "max_duration_sec")
    .orderBy(F.col("avg_duration_sec").desc())
)
