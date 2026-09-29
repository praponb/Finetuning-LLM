from pyspark.sql import functions as F

recent_web_events = (
    df_web_events
    .filter((F.col("event_type") == "purchase") & (F.col("duration_sec") < 405) & (F.col("event_time") < "2024-12-24"))
)

agg = (
    recent_web_events
    .groupBy("event_type")
    .agg(F.count("*").alias("cnt"))
)

result = (
    agg
    .filter(F.col("cnt") >= 6)
    .select("event_type", "cnt")
    .orderBy("event_type")
)
