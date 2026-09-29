from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "search")
    .select(
        "id",
        "event_time",
        "event_type",
        F.when(F.col("duration_sec") >= 428, "High").when(F.col("duration_sec") >= 96, "Medium").otherwise("Low").alias("duration_sec_band"),
    )
)
