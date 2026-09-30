from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "duration_sec",
        "event_type",
        F.round("duration_sec", 1).alias("duration_sec_rounded"),
    )
    .orderBy(F.col("duration_sec_rounded").desc())
)
