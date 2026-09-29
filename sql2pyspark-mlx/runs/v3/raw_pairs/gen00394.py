from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["search", "purchase", "add_to_cart"]) & F.col("event_type").isin(["click", "page_view", "purchase"]))
    .cube(
        F.when(F.col("event_type").isin(["click", "page_view"]), "tier_1").otherwise("other").alias("event_type_group"),
        "event_type",
    )
    .agg(
        F.sum(F.when(F.col("event_type") == "click", F.col("duration_sec")).otherwise(0)).alias("click_duration_sec"),
        F.min("event_time").alias("min_event_time"),
    )
    .orderBy("event_type")
)
