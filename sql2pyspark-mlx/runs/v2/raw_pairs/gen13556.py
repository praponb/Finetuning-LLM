from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") >= 232) & (F.col("duration_sec") < 336))
    .orderBy(F.col("duration_sec").desc())
)
