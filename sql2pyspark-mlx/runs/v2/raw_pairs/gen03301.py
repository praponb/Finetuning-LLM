from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(~(F.col("page").isin(["/search", "/checkout"]) & (F.col("duration_sec") >= 124)))
)

web_events_stats = (
    base
    .groupBy(
        F.year("event_time").alias("event_year"),
        F.when(F.col("duration_sec") >= 448, "high").when(F.col("duration_sec") >= 61, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.max("event_time").alias("last_event_time"),
    )
)

result = (
    web_events_stats
    .filter(F.col("duration_sec_count") >= 7)
    .select(
        "event_year",
        "duration_sec_bucket",
        "duration_sec_count",
        "last_event_time",
    )
    .orderBy("duration_sec_bucket")
)
