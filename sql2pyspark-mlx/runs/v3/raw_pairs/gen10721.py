from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_count"),
        F.max("event_time").alias("last_event_time"),
    )
    .filter(F.col("home_count") > 1)
)
