from pyspark.sql import functions as F

filtered_shipments = df_shipments.filter(F.col("carrier").isin(["DHL", "FedEx"]))

shipments_stats = (
    filtered_shipments
    .groupBy("order_id", "carrier")
    .agg(
        F.count("cost").alias("cost_count"),
        F.countDistinct("cost").alias("unique_orders"),
    )
)

result = shipments_stats
