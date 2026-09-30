from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") != 151)
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.round(((100 * F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0))) / F.count("*")), 1).alias("pct_add_to_cart"),
        F.avg("duration_sec").alias("avg_duration_sec"),
    )
)
