from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(155, 276))
    .select("id", "event_type", "duration_sec")
    .intersect(
        df_web_events
        .filter(F.col("duration_sec") != 312)
        .select("id", "event_type", "duration_sec")
    )
)
