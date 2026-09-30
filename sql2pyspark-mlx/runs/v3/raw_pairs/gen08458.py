from pyspark.sql import functions as F

recent_shipments = df_shipments.filter(F.col("order_id").isNotNull())

shipments_stats = (
    recent_shipments
    .groupBy("status")
    .agg(F.max("shipped_date").alias("latest_shipped_date"))
)

result = (
    shipments_stats
    .select("status", "latest_shipped_date")
    .orderBy("latest_shipped_date")
)
