from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id")
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
    .filter(F.col("duration_sec_range") >= 47)
    .select("session_id", "duration_sec_range")
    .orderBy(F.col("session_id").desc())
)
