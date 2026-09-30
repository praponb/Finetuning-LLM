from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["search", "click"]) & F.col("event_type").isin(["add_to_cart", "page_view", "purchase"]))
    .select("event_time", "page")
)
