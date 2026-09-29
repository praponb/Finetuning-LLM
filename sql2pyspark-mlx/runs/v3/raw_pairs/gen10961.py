from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "click")
    .groupBy(
        "event_type",
        F.when(F.col("event_type").isin(["add_to_cart", "purchase"]), "core").otherwise("other").alias("event_type_group"),
    )
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
    .orderBy("event_type_group")
)
