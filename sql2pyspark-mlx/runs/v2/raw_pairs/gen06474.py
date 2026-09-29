from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") == 301)
    .select("duration_sec", "session_id")
    .unionByName(
        df_web_events
        .filter(F.col("session_id").isin([1, 11, 20, 33]))
        .select("duration_sec", "session_id")
    )
)
