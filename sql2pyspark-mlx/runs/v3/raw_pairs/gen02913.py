from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter(~(F.col("page").isNotNull() & (F.col("page") == "/search")))
    .withColumn("event_time_day", F.dayofmonth("event_time"))
)

page_summary = (
    filtered_web_events
    .groupBy("page")
    .agg(F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"))
)

result = (
    page_summary
    .filter(F.col("avg_duration_sec") > 43)
    .select("page", "avg_duration_sec")
    .orderBy(F.col("avg_duration_sec").desc())
)
