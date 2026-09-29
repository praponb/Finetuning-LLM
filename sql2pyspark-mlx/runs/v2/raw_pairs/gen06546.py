from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~((F.col("page") == "/cart") & F.col("event_type").isin(["page_view", "purchase", "search"])))
    .select("id", "session_id")
)
