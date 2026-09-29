from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(F.col("start_date").alias("event_date"))
    .intersect(df_shipments.select(F.col("shipped_date").alias("event_date")))
)
