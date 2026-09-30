from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type", "page")
    .agg(
        F.sum(F.when(F.col("event_type") == "add_to_cart", F.col("duration_sec")).otherwise(0)).alias("add_to_cart_duration_sec"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
    .filter(F.col("add_to_cart_duration_sec") > 56)
    .orderBy("page")
)
