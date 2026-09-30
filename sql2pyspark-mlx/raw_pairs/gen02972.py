from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(132, 420))
    .select("id", "event_type", "session_id")
    .subtract(
        df_web_events
        .filter(F.col("event_type") == "purchase")
        .select("id", "event_type", "session_id")
    )
    .orderBy("session_id")
)
