from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.hour("event_time").alias("event_hour"),
        F.to_date("event_time").alias("event_day"),
    )
    .agg(
        F.sum(F.when(F.col("page") == "/search", 1).otherwise(0)).alias("search_count"),
    )
    .filter(F.col("search_count") >= 27)
    .orderBy(F.col("event_hour").desc(), "event_day")
    .limit(50)
)
