from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.alias("shipments"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .groupBy(F.col("orders.id"), F.col("orders.status"))
    .agg(
        F.count(F.col("shipments.id")).alias("shipment_count"),
        F.sum(F.when(F.col("shipments.carrier") == "Kerry", 1).otherwise(0)).alias("kerry_count"),
    )
    .orderBy(F.col("id").desc())
)
