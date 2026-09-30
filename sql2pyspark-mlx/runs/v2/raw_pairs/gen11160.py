from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(((F.col("session_id") == 7) & (F.col("duration_sec") <= 158)) | (F.col("event_type") == "purchase"))
    .groupBy(
        "event_type",
        "session_id",
        F.date_format("event_time", "yyyy-MM").alias("event_month"),
    )
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.sum(F.when(F.col("page") == "/checkout", F.col("duration_sec")).otherwise(0)).alias("checkout_duration_sec"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
    .filter(F.col("duration_sec_count") < 3)
    .drop("duration_sec_count")
)
