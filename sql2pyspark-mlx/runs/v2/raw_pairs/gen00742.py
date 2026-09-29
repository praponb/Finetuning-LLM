from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "search") | F.col("duration_sec").isNotNull())
    .groupBy("event_type")
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .orderBy(F.col("total_duration_sec").desc())
)
