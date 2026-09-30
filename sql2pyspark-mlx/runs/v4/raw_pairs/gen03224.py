from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.to_date("event_time") >= "2025-04-01")
    .select(
        "id",
        "page",
        F.round((F.col("duration_sec") * 0.9), 2).alias("adjusted_duration_sec"),
        F.col("duration_sec").cast("string").alias("duration_sec_str"),
    )
)
