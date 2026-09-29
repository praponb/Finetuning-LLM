from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") <= 382) & F.hour("event_time").between(11, 15))
    .groupBy("event_type", "page")
    .agg(
        F.sum(F.when(F.col("page") == "/product", 1).otherwise(0)).alias("product_count"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
    .filter(F.col("avg_duration_sec") >= 46)
)
