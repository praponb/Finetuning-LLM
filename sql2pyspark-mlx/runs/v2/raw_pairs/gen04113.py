from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "purchase")
    .groupBy("page")
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.round(((100 * F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0))) / F.count("*")), 1).alias("pct_page_view"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
    .filter(F.col("avg_duration_sec") < 130)
    .drop("avg_duration_sec")
    .orderBy(F.col("pct_page_view").desc())
)
