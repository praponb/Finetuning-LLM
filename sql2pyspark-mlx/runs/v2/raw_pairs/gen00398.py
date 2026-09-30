from pyspark.sql import functions as F

result = (
    df_web_events
    .cube("event_type", "page")
    .agg(F.min("duration_sec").alias("min_duration_sec"))
    .orderBy(F.col("min_duration_sec").desc())
)
