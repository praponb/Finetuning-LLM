from pyspark.sql import functions as F

src = (
    df_web_events
    .filter(F.col("session_id").isin([8, 32]) & ((F.col("page") != "/product") | F.hour("event_time").between(9, 15)))
)

web_events_stats = (
    src
    .groupBy(F.quarter("event_time").alias("event_quarter"))
    .agg(
        F.sum(F.when(F.col("event_type") == "add_to_cart", F.col("duration_sec")).otherwise(0)).alias("add_to_cart_duration_sec"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
)

result = (
    web_events_stats
    .filter(F.col("add_to_cart_duration_sec") > 63)
    .select("event_quarter", "add_to_cart_duration_sec")
)
