from pyspark.sql import functions as F

base = df_shipments.filter(F.col("order_id").isNotNull())

shipments_stats = (
    base
    .groupBy("status")
    .agg(F.count("*").alias("total_count"))
)

result = (
    shipments_stats
    .filter(F.col("total_count") < 10)
    .orderBy(F.col("status").desc())
)
