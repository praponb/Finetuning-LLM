from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") != "/cart") & (F.col("duration_sec") > 331))
    .groupBy(F.hour("event_time").alias("event_hour"))
    .agg(F.count(F.when(F.col("event_type") == "search", 1)).alias("search_count"))
    .filter(F.col("search_count") >= 5)
)
