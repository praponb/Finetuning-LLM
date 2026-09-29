from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([4, 34]))
    .groupBy(F.hour("event_time").alias("event_hour"))
    .agg(
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
        F.count("*").alias("web_event_count"),
    )
    .filter(F.col("purchase_count") >= 68)
    .orderBy(F.col("event_hour").desc())
)
