from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.quarter("event_time").alias("event_quarter"), "page")
    .agg(
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_count"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 2)
    .orderBy("page")
)
