from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(6, 11) & (F.col("duration_sec") <= 330))
    .withColumn("adjusted_duration_sec", F.round((F.col("duration_sec") * 1.07), 2))
    .withColumn("event_type_title", F.initcap(F.trim("event_type")))
    .orderBy(F.col("event_type_title").desc())
)
