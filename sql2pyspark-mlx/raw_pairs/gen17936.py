from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.to_date("event_time").alias("event_day"))
    .agg(
        F.sum(F.when(F.col("page") == "/home", 1).otherwise(0)).alias("home_count"),
        F.max("event_time").alias("last_event_time"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
    )
    .filter(F.col("purchase_count") >= 1)
    .drop("purchase_count")
    .orderBy("event_day", "last_event_time")
)
