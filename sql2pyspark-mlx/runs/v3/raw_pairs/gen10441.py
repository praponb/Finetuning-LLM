from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") > 394)
    .select("page", "duration_sec")
    .intersect(
        df_web_events
        .filter(F.hour("event_time").between(11, 16))
        .select("page", "duration_sec")
    )
)
