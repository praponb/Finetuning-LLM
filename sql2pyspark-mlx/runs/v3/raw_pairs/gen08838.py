from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") == "/checkout") & (F.col("event_type") != "purchase") & (F.col("session_id") == 1))
    .groupBy(F.month("event_time").alias("event_month"))
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.max("event_time").alias("latest_event_time"),
    )
    .filter(F.col("total_duration_sec") >= 2800)
    .orderBy(F.col("event_month").desc())
)
