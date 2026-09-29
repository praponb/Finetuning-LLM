from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") == "/search") & F.col("page").isin(["/home", "/product", "/search"]) & F.col("duration_sec").isNotNull())
    .groupBy("event_type")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
    .orderBy("event_type")
)
