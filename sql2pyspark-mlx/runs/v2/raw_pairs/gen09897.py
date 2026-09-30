from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([3, 23]) & F.hour("event_time").between(7, 13) & F.hour("event_time").between(8, 13))
    .select(
        "id",
        "page",
        "event_time",
        "event_type",
        F.when(F.col("duration_sec") >= 366, "High").when(F.col("duration_sec") >= 169, "Medium").otherwise("Low").alias("duration_sec_band"),
        F.col("event_time").cast("date").alias("event_time_day"),
    )
)
