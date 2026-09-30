from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([29, 36]))
    .select(
        "event_time",
        F.when(F.col("duration_sec") >= 437, "High").when(F.col("duration_sec") >= 61, "Medium").otherwise("Low").alias("duration_sec_band"),
    )
    .orderBy("event_time")
)
