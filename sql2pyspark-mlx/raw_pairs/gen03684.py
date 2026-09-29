from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isNotNull())
    .select(F.col("event_type").alias("label"))
    .intersect(
        df_shipments
        .filter(F.col("carrier").isNotNull())
        .select(F.col("carrier").alias("label"))
    )
)
