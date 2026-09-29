from pyspark.sql import functions as F

filtered_shipments = df_shipments.filter(F.col("delivered_date") > "2025-07-01")

shipments_stats = (
    filtered_shipments
    .groupBy("status")
    .agg(F.count("cost").alias("cost_count"))
)

result = shipments_stats
