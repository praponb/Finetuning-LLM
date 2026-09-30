from pyspark.sql import functions as F

result = (
    df_web_events
    .select("session_id", "event_type")
    .intersectAll(
        df_web_events
        .filter(F.col("page") != "/checkout")
        .select("session_id", "event_type")
    )
)
