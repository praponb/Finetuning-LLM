from pyspark.sql import functions as F

result = (
    df_web_events
    .agg(
        F.max("event_time").alias("latest_event_time"),
        F.sum(F.when(F.col("page") == "/home", 1).otherwise(0)).alias("home_count"),
    )
    .orderBy(F.col("home_count").desc())
)
