from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([8, 33]))
    .select("id", "session_id", "duration_sec")
    .unionByName(
        df_web_events
        .filter(F.col("duration_sec").isNotNull())
        .select("id", "session_id", "duration_sec")
    )
)
