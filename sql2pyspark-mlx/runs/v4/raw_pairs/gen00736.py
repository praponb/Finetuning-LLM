from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "page_view")
    .select("session_id", "event_type")
    .unionByName(
        df_web_events
        .filter(F.col("event_type") == "add_to_cart")
        .select("session_id", "event_type")
    )
    .distinct()
)
