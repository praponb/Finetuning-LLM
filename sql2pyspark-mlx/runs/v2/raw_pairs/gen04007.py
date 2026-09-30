from pyspark.sql import functions as F

src = (
    df_web_events
    .filter(F.col("duration_sec").between(400, 448) & F.col("duration_sec").isNull() & (F.col("event_type") == "page_view"))
)

summary = (
    src
    .groupBy("event_type")
    .agg(
        F.max("event_time").alias("last_event_time"),
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_count"),
    )
)

result = summary.orderBy(F.col("last_event_time").desc())
