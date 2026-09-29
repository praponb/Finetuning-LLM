from pyspark.sql import functions as F

src = df_web_events.filter((F.col("page") == "/cart") & (F.col("duration_sec") < 179))

agg = (
    src
    .groupBy("event_type", "page")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.min("event_time").alias("min_event_time"),
    )
)

result = (
    agg
    .select("event_type", "page", "min_event_time")
    .orderBy("min_event_time")
)
