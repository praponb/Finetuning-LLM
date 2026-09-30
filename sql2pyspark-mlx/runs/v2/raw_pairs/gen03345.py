from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("event_type").isin(["purchase", "search", "add_to_cart"]) & F.col("session_id").isin([2, 6, 33]))
    .withColumn("event_time_label", F.date_format("event_time", "MMM yyyy"))
    .orderBy(F.col("session_id").desc())
)
