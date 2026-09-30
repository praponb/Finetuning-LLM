from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.alias("shipments"), F.col("orders.id") == F.col("shipments.order_id"), "inner")
    .filter(F.col("orders.channel").isin(["store", "mobile", "web"]))
    .groupBy(F.col("orders.status"))
    .agg(
        F.count(F.col("shipments.id")).alias("shipment_count"),
        F.max(F.col("shipments.cost")).alias("max_cost"),
    )
    .filter(F.col("shipment_count") >= 1)
    .orderBy(F.col("max_cost").desc())
)
