from pyspark.sql import functions as F

filtered = (
    df_web_events
    .filter(F.col("event_type") == "page_view")
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
        F.coalesce("page", F.lit("OTHER")).alias("page_filled"),
    )
)

result = (
    filtered
    .groupBy("page")
    .pivot("event_type", ["add_to_cart", "search"])
    .agg(F.sum("duration_sec").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)
