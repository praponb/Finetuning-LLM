from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") != 459)
    .groupBy("session_id")
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
)
