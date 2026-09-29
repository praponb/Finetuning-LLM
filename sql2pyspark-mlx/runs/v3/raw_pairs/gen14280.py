from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 515, "high").when(F.col("duration_sec") >= 180, "mid").otherwise("low").alias("duration_sec_bucket"),
        "event_type",
    )
    .agg(F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"))
)
