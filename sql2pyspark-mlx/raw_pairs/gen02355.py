from pyspark.sql import functions as F

src = df_web_events.filter(F.col("event_type").isin(["page_view", "purchase"]))

web_events_stats = (
    src
    .groupBy("event_type")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0))) / F.count("*")), 1).alias("pct_click"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
)

result = web_events_stats.orderBy(F.col("avg_duration_sec").desc(), "event_type")
