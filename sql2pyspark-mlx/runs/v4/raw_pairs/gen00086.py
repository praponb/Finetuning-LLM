from pyspark.sql import functions as F

valid = (
    df_web_events
    .filter(F.col("duration_sec") != 301)
    .select(
        "*",
        F.round((F.col("duration_sec") * 0.9), 2).alias("adjusted_duration_sec"),
    )
)

result = (
    valid
    .select("page")
    .exceptAll(
        df_web_events
        .filter(F.hour("event_time").between(7, 10))
        .select("page")
    )
)
