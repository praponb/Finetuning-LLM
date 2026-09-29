from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") > 166)
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("page") == "/product", F.col("duration_sec")).otherwise(0)).alias("product_duration_sec"),
        F.sum(F.when(F.col("event_type") == "search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
        F.sum(F.when(F.col("event_type") == "page_view", F.col("duration_sec")).otherwise(0)).alias("page_view_duration_sec"),
    )
    .filter(F.col("search_duration_sec") > 79)
)
