from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id")
    .agg(F.max("duration_sec").alias("max_duration_sec"))
    .orderBy(F.col("max_duration_sec").desc())
)
