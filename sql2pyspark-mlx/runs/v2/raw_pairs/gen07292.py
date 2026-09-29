from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(215, 468))
    .select("id", "session_id")
    .intersect(
        df_web_events
        .filter(F.col("event_type") == "add_to_cart")
        .select("id", "session_id")
    )
)
