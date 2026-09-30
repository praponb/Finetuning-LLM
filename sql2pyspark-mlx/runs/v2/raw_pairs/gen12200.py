from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.sum(F.when(F.col("event_type") == "add_to_cart", F.col("duration_sec")).otherwise(0)).alias("add_to_cart_duration_sec"),
        F.countDistinct("duration_sec").alias("unique_event_types"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") < 1)
    .drop("cnt")
    .orderBy("event_type")
)
