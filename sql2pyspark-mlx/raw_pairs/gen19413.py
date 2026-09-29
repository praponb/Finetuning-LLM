from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/checkout")
    .withColumn("duration_sec_level", F.when(F.col("duration_sec") >= 503, "High").when(F.col("duration_sec") >= 224, "Medium").otherwise("Low"))
    .withColumn("years_since_event_time", (F.datediff(F.current_date(), "event_time") / 365.25))
    .orderBy(F.col("id").desc(), F.col("session_id").desc())
    .limit(5)
)
