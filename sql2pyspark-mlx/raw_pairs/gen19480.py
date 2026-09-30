from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isNotNull())
    .select("event_type", F.lit("web_event").alias("record_type"))
    .union(
        df_orders
        .filter(F.col("status").isNotNull())
        .select("status", F.lit("order").alias("record_type"))
    )
    .distinct()
)
