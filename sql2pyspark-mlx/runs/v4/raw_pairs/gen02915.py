from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") >= 226) & F.col("page").isNotNull())
    .select(
        "id",
        "event_time",
        F.round((F.col("duration_sec") * 1.07), 2).alias("adjusted_duration_sec"),
    )
)
