from pyspark.sql import functions as F

recent_web_events = (
    df_web_events
    .filter((F.col("duration_sec") <= 452) & (F.col("event_time") >= "2024-05-20") & F.col("duration_sec").between(162, 175))
)

summary = (
    recent_web_events
    .groupBy("event_type")
    .agg(
        F.max("event_time").alias("last_event_time"),
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
    )
)

result = (
    summary
    .select("event_type", "last_event_time", "page_values")
    .orderBy(F.col("last_event_time").desc())
)
