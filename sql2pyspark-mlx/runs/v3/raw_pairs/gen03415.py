from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("orders.order_date") < "2023-03-14")
    .groupBy(F.col("orders.status"), F.col("orders.channel"))
    .agg(
        F.sum(F.when(F.col("shipments.carrier") == "FedEx", F.col("shipments.cost")).otherwise(0)).alias("fedex_cost"),
        F.count("*").alias("shipment_count"),
    )
    .orderBy(F.col("shipment_count").desc(), "status", "channel")
    .limit(50)
)
