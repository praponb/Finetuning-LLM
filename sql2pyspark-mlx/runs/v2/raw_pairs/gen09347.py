from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["page_view", "search", "click"]) & F.col("event_type").isin(["click", "purchase"]) & (F.col("duration_sec") >= 228))
    .groupBy("event_type", "page")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0))) / F.count("*")), 1).alias("pct_purchase"),
    )
    .orderBy("pct_purchase", "event_type")
)
