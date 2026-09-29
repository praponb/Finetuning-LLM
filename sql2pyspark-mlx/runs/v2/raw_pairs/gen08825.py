from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").isNotNull() & (F.col("event_type") == "page_view"))
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
        F.count("*").alias("total_count"),
    )
    .orderBy("total_count", "event_month")
    .limit(10)
)
