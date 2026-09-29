from pyspark.sql import functions as F

src = (
    df_web_events
    .filter(F.hour("event_time").between(11, 19) | (F.col("duration_sec") > 134))
)

event_type_summary = (
    src
    .groupBy("event_type")
    .agg(F.max("duration_sec").alias("max_duration_sec"))
)

result = (
    event_type_summary
    .filter(F.col("max_duration_sec") >= 380)
    .select("event_type", "max_duration_sec")
    .orderBy("event_type")
    .limit(10)
)
