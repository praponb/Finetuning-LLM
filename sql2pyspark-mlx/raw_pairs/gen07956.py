from pyspark.sql import functions as F

filtered_shipments = (
    df_shipments
    .filter(F.col("status") != "IN_TRANSIT")
    .withColumn("cost_band", F.when(F.col("cost") >= 105, "High").when(F.col("cost") >= 26, "Medium").otherwise("Low"))
)

shipments_stats = (
    filtered_shipments
    .groupBy("carrier")
    .agg(F.countDistinct("cost").alias("unique_orders"))
)

result = (
    shipments_stats
    .select("carrier", "unique_orders")
    .orderBy("carrier")
)
