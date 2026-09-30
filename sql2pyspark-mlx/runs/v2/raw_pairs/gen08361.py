from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.quarter("event_time").alias("event_quarter"))
    .agg(
        F.sum(F.when(F.col("page") == "/search", 1).otherwise(0)).alias("search_count"),
    )
    .filter(F.col("search_count") < 59)
    .orderBy(F.col("event_quarter").desc(), F.col("search_count").desc())
)
