from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("event_type").isin(["add_to_cart", "purchase", "page_view"]))
    .select("id", "session_id", "duration_sec")
    .subtract(
        df_web_events
        .filter(F.col("event_time") < "2023-06-03")
        .select("id", "session_id", "duration_sec")
    )
)
