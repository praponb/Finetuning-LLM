from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/product", "/search"]))
    .groupBy("page", "event_type")
    .agg(
        F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0)).alias("page_view_count"),
    )
    .filter(F.col("page_view_count") < 62)
    .select("page", "event_type", "page_view_count")
)
