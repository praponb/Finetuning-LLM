from pyspark.sql import functions as F

result = (
    df_web_events
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
)
