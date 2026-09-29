from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") == "/product") & ~F.col("event_type").isin(["add_to_cart", "click", "search"]))
    .select("event_time", "session_id", "event_type")
)
