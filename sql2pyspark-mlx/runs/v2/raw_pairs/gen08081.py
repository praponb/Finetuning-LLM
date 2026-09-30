from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.quarter("event_time").alias("event_quarter"))
    .agg(
        F.max("event_time").alias("latest_event_time"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", F.col("duration_sec")).otherwise(0)).alias("add_to_cart_duration_sec"),
    )
    .orderBy(F.col("event_quarter").desc())
)
