from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([2, 27, 31]))
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.round(((100 * F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
    )
)
