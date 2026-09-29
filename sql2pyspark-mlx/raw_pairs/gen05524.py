from pyspark.sql import functions as F

result = (
    df_web_events
    .select("session_id", "id")
    .subtract(
        df_web_events
        .filter(F.col("event_time") >= "2024-10-29")
        .select("session_id", "id")
    )
)
