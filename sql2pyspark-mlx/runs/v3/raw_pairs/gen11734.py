from pyspark.sql import functions as F

src = (
    df_web_events
    .filter((F.col("event_time") >= "2024-04-13") & F.col("session_id").isin([4, 6, 16]))
    .withColumn("event_time_date", F.to_date("event_time"))
)

summary = (
    src
    .groupBy("event_type")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.count("*").alias("cnt"),
    )
)

result = (
    summary
    .filter(F.col("avg_duration_sec") > 67)
    .select("event_type", "avg_duration_sec", "cnt")
)
