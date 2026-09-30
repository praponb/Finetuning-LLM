from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") > 460) & F.hour("event_time").between(11, 15))
    .select(
        "id",
        "event_time",
        F.date_trunc("WEEK", "event_time").alias("event_time_period"),
    )
)
