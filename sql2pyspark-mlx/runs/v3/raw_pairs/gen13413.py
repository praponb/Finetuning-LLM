from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["page_view", "add_to_cart"]) & F.col("duration_sec").isNotNull())
    .groupBy("event_type")
    .agg(
        F.sum(F.when(F.col("page") == "/product", 1).otherwise(0)).alias("product_count"),
    )
    .orderBy(F.col("product_count").desc(), "event_type")
)
