from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page", "event_type")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.min("duration_sec").alias("min_duration_sec"),
        F.round(((100 * F.sum(F.when(F.col("page") == "/home", 1).otherwise(0))) / F.count("*")), 1).alias("pct_home"),
    )
    .filter(F.col("min_duration_sec") < 180)
)
