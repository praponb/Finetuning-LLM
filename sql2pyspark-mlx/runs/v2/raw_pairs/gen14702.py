from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") != "/product")
    .groupBy(
        F.when(F.col("duration_sec") >= 479, "high").when(F.col("duration_sec") >= 152, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.max("event_time").alias("latest_event_time"),
    )
)
