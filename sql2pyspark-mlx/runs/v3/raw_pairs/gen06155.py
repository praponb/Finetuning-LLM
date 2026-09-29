from pyspark.sql import functions as F

recent_shipments = df_shipments.filter(F.col("order_id").isin([11, 13, 23, 30]))

shipments_stats = (
    recent_shipments
    .groupBy("status")
    .agg(F.max("delivered_date").alias("last_delivered_date"))
)

result = (
    shipments_stats
    .select("status", "last_delivered_date")
    .orderBy("status", F.col("last_delivered_date").desc())
    .limit(20)
)
