from pyspark.sql import functions as F

result = (
    df_web_events
    .select("session_id", "id")
    .intersectAll(
        df_web_events
        .filter(F.col("duration_sec") != 447)
        .select("session_id", "id")
    )
)
