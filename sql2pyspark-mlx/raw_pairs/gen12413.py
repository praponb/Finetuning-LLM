from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter(F.col("order_id").isNull())
    .withColumn("days_delivered_date_to_shipped_date", F.datediff("shipped_date", "delivered_date"))
)

summary = (
    recent_shipments
    .groupBy("carrier")
    .agg(
        F.min("days_delivered_date_to_shipped_date").alias("min_days_delivered_date_to_shipped_date"),
    )
)

result = (
    summary
    .select("carrier", "min_days_delivered_date_to_shipped_date")
    .orderBy("carrier")
)
