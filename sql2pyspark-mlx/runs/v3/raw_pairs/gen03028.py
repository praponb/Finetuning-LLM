from pyspark.sql import functions as F

recent_web_events = df_web_events.filter(F.col("event_type").isin(["purchase", "search", "click"]))

agg = (
    recent_web_events
    .groupBy("page")
    .agg(F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"))
)

result = agg.filter(F.col("stddev_duration_sec") > 36)
