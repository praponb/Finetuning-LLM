from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") == "/checkout") & (F.col("event_type") != "page_view") & (F.to_date("event_time") < "2024-06-01"))
    .groupBy("event_type", F.month("event_time").alias("event_month"))
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
    .filter(F.col("duration_sec_range") >= 71)
)
