from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "page_view")
    .select(
        "duration_sec",
        "event_time",
        F.round((F.col("duration_sec") * 1.15), 2).alias("duration_sec_adjusted"),
    )
    .orderBy(F.col("duration_sec").desc(), "event_time")
)
