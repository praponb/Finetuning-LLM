from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") >= "2025-05-16")
    .select(
        "id",
        "event_type",
        F.date_trunc("month", "event_time").alias("event_time_period"),
    )
    .orderBy(F.col("event_time_period").desc(), "id")
    .limit(10)
)
