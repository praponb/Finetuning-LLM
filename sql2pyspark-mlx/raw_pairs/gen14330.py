from pyspark.sql import functions as F

valid_web_events = df_web_events.filter(F.col("page").isin(["/home", "/checkout"]))

agg = (
    valid_web_events
    .groupBy("session_id")
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.round(((100 * F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0))) / F.count("*")), 1).alias("pct_click"),
    )
)

result = (
    agg
    .select("session_id", "first_event_time", "pct_click")
    .orderBy("first_event_time")
)
