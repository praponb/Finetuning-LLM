from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([1, 23, 40]))
    .groupBy("page")
    .agg(F.min("duration_sec").alias("min_duration_sec"))
    .orderBy("page")
)
