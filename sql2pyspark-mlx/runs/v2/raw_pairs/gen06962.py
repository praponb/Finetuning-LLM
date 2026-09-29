from pyspark.sql import functions as F

filtered_shipments = df_shipments.filter((F.col("cost") > 50) & F.col("order_id").isNull())

summary = (
    filtered_shipments
    .groupBy("status")
    .agg(
        F.countDistinct("cost").alias("unique_carriers"),
        F.count("cost").alias("cost_count"),
    )
)

result = summary.select("status", "unique_carriers", "cost_count")
