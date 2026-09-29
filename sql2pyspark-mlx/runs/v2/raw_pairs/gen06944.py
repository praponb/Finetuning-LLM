from pyspark.sql import functions as F

recent_web_events = df_web_events.filter(F.col("duration_sec") >= 242)

summary = (
    recent_web_events
    .groupBy("page")
    .agg(F.count("duration_sec").alias("duration_sec_count"))
)

result = (
    summary
    .select("page", "duration_sec_count")
    .orderBy(F.col("page").desc())
)
