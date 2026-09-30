from pyspark.sql import functions as F

filtered_web_events = df_web_events.filter(F.col("event_type") != "purchase")

summary = (
    filtered_web_events
    .groupBy("event_type")
    .agg(
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_count"),
    )
)

result = (
    summary
    .filter(F.col("page_view_count") > 8)
    .select("event_type", "page_view_count")
    .orderBy(F.col("page_view_count").desc(), "event_type")
    .limit(20)
)
