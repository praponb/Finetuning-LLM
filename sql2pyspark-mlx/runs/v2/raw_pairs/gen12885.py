from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "search") & F.col("session_id").isin([14, 23, 24, 33]))
    .groupBy("event_type", "page")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("stddev_duration_sec") < 26)
    .orderBy("page")
)
