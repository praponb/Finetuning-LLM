from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_count"),
    )
    .orderBy(F.col("search_count").desc())
)
