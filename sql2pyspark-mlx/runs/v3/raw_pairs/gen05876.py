from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") == "/product") & F.col("page").isin(["/home", "/product", "/search"]) & (F.col("event_time") >= "2024-07-01"))
    .groupBy("page")
    .agg(
        F.max("event_time").alias("latest_event_time"),
        F.sum("duration_sec").alias("total_duration_sec"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
    .filter(F.col("total_duration_sec") > 900)
    .orderBy(F.col("latest_event_time").desc(), "page")
    .limit(10)
)
