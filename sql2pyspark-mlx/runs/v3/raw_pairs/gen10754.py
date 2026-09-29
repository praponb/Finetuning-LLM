from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter(F.col("page").isin(["/checkout", "/search", "/cart"]))
    .withColumn("event_time_hour", F.hour("event_time"))
)

event_hour_summary = (
    filtered_web_events
    .groupBy(F.hour("event_time").alias("event_hour"))
    .agg(F.max("duration_sec").alias("max_duration_sec"))
)

result = event_hour_summary.select("event_hour", "max_duration_sec")
