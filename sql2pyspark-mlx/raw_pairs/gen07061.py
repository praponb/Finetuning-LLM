from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "duration_sec",
        "event_type",
        "page",
        F.round((F.col("duration_sec") * 0.9), 2).alias("duration_sec_adjusted"),
    )
)
