from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type", "page")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_count"),
    )
    .orderBy("page", F.col("avg_duration_sec").desc())
)
