from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") == 132)
    .groupBy("page")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
)
