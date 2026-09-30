from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
    .filter(F.col("max_duration_sec") >= 340)
    .orderBy(F.col("max_duration_sec").desc(), "event_type")
    .limit(20)
)
