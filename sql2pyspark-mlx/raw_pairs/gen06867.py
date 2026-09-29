from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") != 129)
    .select(
        "id",
        "event_type",
        (F.datediff(F.current_date(), "event_time") / 365.25).alias("years_since_event_time"),
        F.ceil("duration_sec").alias("duration_sec_whole"),
    )
)
