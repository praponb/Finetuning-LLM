from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.to_date("event_time") >= "2025-08-01") & (F.col("event_time") < "2023-05-16"))
    .withColumn("duration_sec_band", F.when(F.col("duration_sec") >= 461, "High").when(F.col("duration_sec") >= 165, "Medium").otherwise("Low"))
)
