from pyspark.sql import functions as F

base = (
    df_web_events
    .filter((F.col("event_time") >= "2023-04-15") & (F.col("event_time") < "2024-02-01"))
)

event_type_summary = (
    base
    .groupBy("event_type")
    .agg(
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
        F.min("event_time").alias("first_event_time"),
    )
)

result = (
    event_type_summary
    .filter(F.col("purchase_count") < 49)
    .select("event_type", "first_event_time")
)
