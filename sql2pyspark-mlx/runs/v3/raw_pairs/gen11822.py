from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([14, 15, 27, 34]) & ((F.col("duration_sec") <= 282) | (F.to_date("event_time") < "2024-10-12")))
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
    .orderBy(F.col("duration_sec_range").desc())
)
