from pyspark.sql import functions as F

recent_web_events = (
    df_web_events
    .filter(F.col("page").isin(["/product", "/cart", "/checkout"]) & (F.col("event_time") < "2023-05-01") & (F.col("event_type") == "purchase"))
)

web_events_stats = (
    recent_web_events
    .groupBy(
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
    )
    .agg(
        F.countDistinct("duration_sec").alias("unique_sessions"),
        F.min("event_time").alias("first_event_time"),
    )
)

result = web_events_stats.orderBy("event_cohort")
