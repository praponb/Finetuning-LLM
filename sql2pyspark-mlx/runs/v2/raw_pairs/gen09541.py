from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["page_view", "add_to_cart"]) & (F.col("session_id").isin([17, 29]) | (F.col("event_time") < "2024-02-08")))
)
