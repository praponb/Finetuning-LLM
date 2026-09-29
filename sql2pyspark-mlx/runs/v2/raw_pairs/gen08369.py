from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") != 355)
    .orderBy(F.col("event_type").desc())
)
