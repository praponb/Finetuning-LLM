from pyspark.sql import functions as F

base = (
    df_web_events
    .filter((F.col("event_time") < "2024-08-18") & ((F.col("page") == "/cart") | (F.col("duration_sec") >= 348)))
)

web_events_stats = (
    base
    .groupBy(
        F.when(F.col("duration_sec") >= 361, "high").when(F.col("duration_sec") >= 210, "mid").otherwise("low").alias("duration_sec_bucket"),
        "page",
    )
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
)

result = (
    web_events_stats
    .filter(F.col("avg_duration_sec") >= 240)
    .select("duration_sec_bucket", "page", "avg_duration_sec")
)
