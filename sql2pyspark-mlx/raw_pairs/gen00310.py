from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter(F.col("event_type").isin(["page_view", "click", "add_to_cart"]) & (F.col("page") == "/checkout") & (F.to_date("event_time") < "2023-03-01"))
)

summary = (
    filtered_web_events
    .groupBy(F.to_date("event_time").alias("event_day"), "session_id")
    .agg(F.count("*").alias("row_count"))
)

result = (
    summary
    .select("event_day", "session_id", "row_count")
    .orderBy(F.col("row_count").desc())
)
