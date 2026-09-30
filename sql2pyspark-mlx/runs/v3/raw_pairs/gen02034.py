from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([5, 19, 28]))
    .select("id", "page", "session_id")
    .intersect(
        df_web_events
        .filter(F.col("event_type") == "purchase")
        .select("id", "page", "session_id")
    )
)
