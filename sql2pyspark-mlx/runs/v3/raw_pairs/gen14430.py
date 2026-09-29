from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("shipments.order_id"), "inner")
    .filter(F.col("orders.status") == "SHIPPED")
    .select(
        F.col("shipments.id"),
        F.col("shipments.cost"),
        F.col("orders.status"),
        F.when(F.col("shipments.cost") >= 76, "High").when(F.col("shipments.cost") >= 23, "Medium").otherwise("Low").alias("cost_level"),
    )
)
