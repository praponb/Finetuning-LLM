from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([10, 13, 36]) & (F.to_date("event_time") >= "2024-03-15"))
    .groupBy("page", F.date_trunc("MONTH", "event_time").alias("event_month"))
    .agg(
        F.sum(F.when(F.col("page") == "/search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
    )
    .filter(F.col("search_duration_sec") < 35)
)
