from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 2)
    .groupBy("event_type")
    .agg(
        F.sum(F.when(F.col("page") == "/home", F.col("duration_sec")).otherwise(0)).alias("home_duration_sec"),
        F.max("duration_sec").alias("max_duration_sec"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
    .filter(F.col("home_duration_sec") >= 32)
    .orderBy("event_type")
)
