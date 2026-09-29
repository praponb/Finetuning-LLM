from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "event_time",
        F.when(F.col("duration_sec") >= 371, "High").when(F.col("duration_sec") >= 93, "Medium").otherwise("Low").alias("duration_sec_band"),
    )
    .orderBy("duration_sec_band")
)
