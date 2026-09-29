from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isNull())
    .select(
        "id",
        "event_time",
        F.round((F.col("duration_sec") * 0.85), 2).alias("adjusted_duration_sec"),
    )
)
