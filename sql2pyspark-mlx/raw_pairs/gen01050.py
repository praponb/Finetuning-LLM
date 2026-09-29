from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id") == 24) & (F.col("page") != "/home"))
    .agg(
        F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
        F.max("event_time").alias("latest_event_time"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
    )
    .orderBy(F.col("page_view_count").desc())
)
