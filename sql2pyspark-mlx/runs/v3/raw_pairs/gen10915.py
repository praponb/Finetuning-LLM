from pyspark.sql import functions as F

base = df_web_events.filter(F.col("page").isin(["/product", "/search"]))

event_type_summary = (
    base
    .groupBy("event_type", F.date_format("event_time", "yyyy-MM").alias("event_month"))
    .agg(F.count("*").alias("row_count"))
)

result = (
    event_type_summary
    .filter(F.col("row_count") >= 3)
    .orderBy(F.col("row_count").desc())
)
