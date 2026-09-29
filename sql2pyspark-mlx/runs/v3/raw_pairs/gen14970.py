from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["click", "search"]) & (F.col("page") != "/checkout"))
    .withColumn("event_time_label", F.date_format("event_time", "MMM yyyy"))
    .orderBy("duration_sec")
)
