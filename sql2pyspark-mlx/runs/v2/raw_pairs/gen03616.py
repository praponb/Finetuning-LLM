from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") < "2023-05-01") & F.col("page").isin(["/checkout", "/home", "/search"]))
    .cube(F.month("event_time").alias("event_month"), "page")
    .agg(
        F.max("event_time").alias("latest_event_time"),
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
    .orderBy("min_duration_sec", "avg_duration_sec")
)
