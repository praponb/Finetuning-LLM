from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([1, 5, 6, 34]))
    .select("duration_sec", "event_type")
    .unionByName(
        df_web_events
        .filter(F.col("page") == "/search")
        .select("duration_sec", "event_type")
    )
)
