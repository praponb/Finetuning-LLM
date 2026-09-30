from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") != 398) & ~F.col("page").isin(["/product", "/checkout"]))
    .groupBy(F.month("event_time").alias("event_month"))
    .agg(F.min("duration_sec").alias("min_duration_sec"))
    .orderBy("min_duration_sec")
)
