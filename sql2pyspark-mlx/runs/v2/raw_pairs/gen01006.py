from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(7, 15) & F.col("session_id").isin([6, 15, 31]) & F.col("page").isin(["/cart", "/search"]))
    .groupBy("event_type")
    .agg(
        F.count("*").alias("row_count"),
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
    .orderBy(F.col("avg_duration_sec").desc(), "event_type")
    .limit(20)
)
