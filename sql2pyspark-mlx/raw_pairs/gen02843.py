from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").isNotNull())
    .withColumn("event_type_lower", F.lower("event_type"))
)
