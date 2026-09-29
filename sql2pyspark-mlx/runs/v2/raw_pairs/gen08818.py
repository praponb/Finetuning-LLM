from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([19, 27, 30, 37]))
    .groupBy("event_type")
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
    .filter(F.col("avg_duration_sec") < 300)
)
