from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("event_type").isin(["add_to_cart", "click"]), "priority").otherwise("other").alias("event_type_group"),
    )
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
    .filter(F.col("stddev_duration_sec") < 40)
)
