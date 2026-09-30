from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(F.col("end_date").alias("event_date"))
    .subtract(df_shipments.select(F.col("shipped_date").alias("event_date")))
    .orderBy("event_date")
)
