from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id")
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
    .filter(F.col("total_duration_sec") > 1700)
    .orderBy(
        F.col("duration_sec_range").desc(),
        F.col("total_duration_sec").desc(),
        "session_id",
    )
    .limit(10)
)
