from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.min("event_time").alias("min_event_time"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 2)
    .drop("cnt")
    .orderBy("min_event_time", "event_type")
    .limit(5)
)
