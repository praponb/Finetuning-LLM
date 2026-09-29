from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.to_date("event_time") < "2023-10-23") & (F.col("session_id") == 12))
    .rollup(F.date_trunc("MONTH", "event_time").alias("event_month"), "session_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.count("*").alias("total_count"),
    )
)
