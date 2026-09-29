from pyspark.sql import functions as F

recent_web_events = (
    df_web_events
    .filter((F.col("duration_sec") != 198) | F.col("event_type").isin(["search", "page_view"]))
    .withColumn("event_time_year", F.year("event_time"))
)

agg = (
    recent_web_events
    .groupBy(F.to_date("event_time").alias("event_day"), "session_id")
    .agg(F.countDistinct("event_time_year").alias("unique_sessions"))
)

result = (
    agg
    .filter(F.col("unique_sessions") > 8)
    .orderBy("event_day", "session_id")
    .limit(10)
)
