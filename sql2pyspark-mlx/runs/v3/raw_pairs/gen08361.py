from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(213, 238))
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
    .orderBy(F.col("duration_sec_range").desc())
)
