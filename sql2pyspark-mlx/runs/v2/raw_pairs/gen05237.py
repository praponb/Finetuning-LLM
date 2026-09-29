from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "purchase")
    .select("id", "event_type", "duration_sec")
    .unionByName(
        df_web_events
        .filter(F.col("session_id") == 5)
        .select("id", "event_type", "duration_sec")
    )
)
