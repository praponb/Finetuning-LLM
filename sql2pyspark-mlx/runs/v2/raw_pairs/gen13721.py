from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([25, 37, 40]) | F.col("event_type").isin(["page_view", "click", "purchase"]))
    .select("id", "page", "event_type", F.substring("page", 1, 3).alias("page_prefix"))
    .orderBy("page_prefix", "id")
    .limit(5)
)
