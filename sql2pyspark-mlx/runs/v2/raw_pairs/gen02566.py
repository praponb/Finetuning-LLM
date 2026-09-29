from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([19, 40]) & F.col("duration_sec").between(234, 462))
    .groupBy("event_type")
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
)
