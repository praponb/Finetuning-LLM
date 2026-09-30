from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([11, 12, 31, 34]))
    .groupBy("page")
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_count"),
    )
    .filter(F.col("page_view_count") < 7)
    .select("page", "first_event_time", "page_view_count")
    .orderBy(F.col("first_event_time").desc(), "page")
    .limit(10)
)
