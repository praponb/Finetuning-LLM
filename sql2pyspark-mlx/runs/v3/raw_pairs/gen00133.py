from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") >= 176) & (F.col("duration_sec") <= 345))
    .groupBy("session_id", "page")
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
    .filter(F.col("min_duration_sec") >= 340)
)
