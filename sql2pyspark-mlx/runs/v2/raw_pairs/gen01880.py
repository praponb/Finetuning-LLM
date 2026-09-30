from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id") == 9) & ((F.col("page") == "/product") | F.col("event_type").isin(["purchase", "search", "add_to_cart"])))
    .groupBy("session_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "page_view", 1).otherwise(0))) / F.count("*")), 1).alias("pct_page_view"),
    )
    .filter(F.col("pct_page_view") < 57)
    .orderBy(F.col("pct_page_view").desc())
)
