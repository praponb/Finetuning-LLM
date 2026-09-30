from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isNotNull())
    .select(F.col("city").alias("label"))
    .unionByName(
        df_web_events
        .filter(F.col("event_type").isNotNull())
        .select(F.col("event_type").alias("label"))
    )
)
