from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") == 352)
    .select("id", "session_id", "event_type")
    .unionByName(
        df_web_events
        .filter(F.col("page").isNull())
        .select("id", "session_id", "event_type")
    )
    .distinct()
)
