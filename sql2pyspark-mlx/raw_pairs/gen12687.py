from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["purchase", "add_to_cart"]) | (F.to_date("event_time") < "2023-09-09"))
    .groupBy("event_type")
    .agg(
        F.min("event_time").alias("min_event_time"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", F.col("duration_sec")).otherwise(0)).alias("add_to_cart_duration_sec"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
)
