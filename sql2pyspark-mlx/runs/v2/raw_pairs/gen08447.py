from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").isNull() & (F.hour("event_time").between(10, 16) | F.hour("event_time").between(11, 19)))
    .groupBy("page")
    .agg(
        F.countDistinct("duration_sec").alias("unique_event_types"),
        F.min("duration_sec").alias("min_duration_sec"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
    )
    .orderBy(F.col("purchase_count").desc())
)
