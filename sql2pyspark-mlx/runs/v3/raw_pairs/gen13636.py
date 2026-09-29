from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(10, 18))
    .groupBy("page")
    .agg(F.count(F.when(F.col("event_type") == "search", 1)).alias("search_count"))
    .orderBy("page")
)
