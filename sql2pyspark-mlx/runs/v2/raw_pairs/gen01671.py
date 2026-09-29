from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        "page",
        F.date_trunc("MONTH", "event_time").alias("event_month"),
        "event_type",
    )
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .orderBy(F.col("page").desc(), "event_month", "event_type")
    .limit(10)
)
