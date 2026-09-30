from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~(F.col("session_id").isin([4, 9, 23, 31]) & ((F.col("page") == "/product") | (F.col("event_time") < "2024-11-01"))))
    .select(
        "event_type",
        F.floor("duration_sec").alias("duration_sec_whole"),
        F.date_trunc("MONTH", "event_time").alias("event_time_period"),
    )
    .orderBy("event_type")
)
