from pyspark.sql import functions as F

result = (
    df_web_events
    .select("session_id")
    .intersect(
        df_web_events
        .filter(F.col("event_type") != "purchase")
        .select("session_id")
    )
)
