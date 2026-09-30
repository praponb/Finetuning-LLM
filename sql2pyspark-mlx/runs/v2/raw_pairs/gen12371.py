from pyspark.sql import functions as F

src = (
    df_web_events
    .filter(F.col("session_id").isin([28, 29, 38]) & F.hour("event_time").between(12, 17))
    .withColumn("event_time_day", F.col("event_time").cast("date"))
)

event_type_summary = (
    src
    .groupBy("event_type")
    .agg(F.min("duration_sec").alias("min_duration_sec"))
)

result = (
    event_type_summary
    .select("event_type", "min_duration_sec")
    .orderBy("min_duration_sec", F.col("event_type").desc())
    .limit(5)
)
