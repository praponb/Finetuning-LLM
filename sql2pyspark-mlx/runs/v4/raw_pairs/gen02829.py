from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "purchase")
    .select("id", "duration_sec", "session_id")
    .unionByName(
        df_web_events
        .filter(F.col("session_id").isin([15, 30]))
        .select("id", "duration_sec", "session_id")
    )
    .orderBy("id")
)
