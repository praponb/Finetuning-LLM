from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id")
    .pivot("event_type", ["add_to_cart", "click"])
    .agg(
        F.sum("duration_sec").alias("value_sum"),
        F.count(F.lit(1)).alias("item_count"),
    )
)
