from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") < 188) | (F.to_date("event_time") < "2024-05-01"))
    .select(
        "duration_sec",
        "event_type",
        "event_time",
        F.round((F.col("duration_sec") * 1.1), 2).alias("duration_sec_adjusted"),
    )
)
