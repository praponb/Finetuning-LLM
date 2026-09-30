from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["add_to_cart", "search", "purchase"]) | (F.col("duration_sec") < 340))
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        F.substring("event_type", 1, 3).alias("event_type_prefix"),
    )
)
