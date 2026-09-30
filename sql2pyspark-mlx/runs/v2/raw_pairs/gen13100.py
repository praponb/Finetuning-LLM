from pyspark.sql import functions as F

base = df_web_events.filter(F.col("duration_sec") != 124)

session_id_summary = (
    base
    .groupBy("session_id")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
)

result = session_id_summary.filter(F.col("total_duration_sec") > 1100)
