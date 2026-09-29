from pyspark.sql import functions as F

valid_shipments = df_shipments.filter(F.col("order_id").isNotNull())

shipments_stats = (
    valid_shipments
    .groupBy("carrier", "status")
    .agg(F.sum("cost").alias("total_cost"))
)

result = (
    shipments_stats
    .select("carrier", "status", "total_cost")
    .orderBy("status")
)
