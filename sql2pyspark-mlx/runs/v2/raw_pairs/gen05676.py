from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(217, 391) & (F.col("event_type").isin(["purchase", "add_to_cart"]) | (F.col("event_type") == "search")))
    .agg(
        F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0)).alias("search_count"),
    )
    .orderBy(F.col("search_count").desc())
)
