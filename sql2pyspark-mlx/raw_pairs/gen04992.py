from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") <= 380) & F.col("event_type").isin(["purchase", "add_to_cart", "page_view"]))
    .select(
        "id",
        "session_id",
        "duration_sec",
        "event_time",
        F.substring("page", 1, 3).alias("page_prefix"),
    )
)
