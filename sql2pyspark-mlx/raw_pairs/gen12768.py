from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") == "purchase") & F.col("event_type").isin(["purchase", "add_to_cart", "click"]))
    .groupBy("page")
    .agg(
        F.countDistinct("duration_sec").alias("unique_sessions"),
        F.countDistinct("duration_sec").alias("unique_event_types"),
        F.min("event_time").alias("min_event_time"),
    )
    .filter(F.col("unique_event_types") > 2)
    .orderBy(F.col("unique_event_types").desc())
)
