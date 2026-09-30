from pyspark.sql import functions as F

recent_web_events = (
    df_web_events
    .filter(F.col("page") != "/home")
    .withColumn("page_length", F.length("page"))
)

web_events_stats = (
    recent_web_events
    .groupBy("session_id")
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
)

result = (
    web_events_stats
    .filter(F.col("avg_duration_sec") >= 200)
    .orderBy(F.col("avg_duration_sec").desc(), F.col("max_duration_sec").desc())
)
