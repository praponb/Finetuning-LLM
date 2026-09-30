from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"), "session_id")
    .agg(
        F.max("event_time").alias("last_event_time"),
        F.round(((100 * F.sum(F.when(F.col("page") == "/search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
    )
    .filter(F.col("pct_search") < 73)
    .orderBy(F.col("last_event_time").desc())
)
