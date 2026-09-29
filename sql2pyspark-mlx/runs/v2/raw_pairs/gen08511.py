from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["page_view", "purchase", "add_to_cart"]) & ((F.to_date("event_time") >= "2024-06-01") | F.col("session_id").isin([2, 12, 31, 35])))
    .withColumn("event_time_date", F.to_date("event_time"))
    .withColumn("event_time_date_2", F.to_date("event_time"))
)
