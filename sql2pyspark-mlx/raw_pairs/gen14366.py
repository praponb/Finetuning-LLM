from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["page_view", "add_to_cart"]))
    .select("event_type")
    .unionByName(df_web_events.filter(F.col("session_id") == 8).select("event_type"))
    .distinct()
)
