from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isNull())
    .select("event_type", "duration_sec")
    .intersect(
        df_web_events
        .filter(F.col("duration_sec") != 184)
        .select("event_type", "duration_sec")
    )
    .orderBy(F.col("event_type").desc())
)
