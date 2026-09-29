from pyspark.sql import functions as F

result = (
    df_web_events
    .select("page", "session_id")
    .intersectAll(
        df_web_events
        .filter(F.col("duration_sec") >= 478)
        .select("page", "session_id")
    )
)
