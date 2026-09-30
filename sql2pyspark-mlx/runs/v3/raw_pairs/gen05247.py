from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([10, 20, 28]) & ((F.to_date("event_time") >= "2024-08-01") | (F.col("event_type") == "click")))
    .select(
        "duration_sec",
        "event_type",
        "page",
        F.when(F.col("duration_sec") >= 449, "High").when(F.col("duration_sec") >= 208, "Medium").otherwise("Low").alias("duration_sec_band"),
    )
    .orderBy(F.col("page").desc())
)
