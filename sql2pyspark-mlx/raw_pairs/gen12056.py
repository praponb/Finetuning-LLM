from pyspark.sql import functions as F

base = df_web_events.filter(F.to_date("event_time") >= "2025-10-01")

page_summary = (
    base
    .groupBy("page")
    .agg(
        F.sum(F.when(F.col("page") == "/cart", 1).otherwise(0)).alias("cart_count"),
        F.count("*").alias("web_event_count"),
    )
)

result = (
    page_summary
    .select("page", "cart_count", "web_event_count")
    .orderBy(F.col("page").desc())
)
