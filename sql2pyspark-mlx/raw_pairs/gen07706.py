from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .filter((F.year(F.col("shipments.shipped_date")) >= 2024) | F.col("shipments.order_id").isNotNull())
    .select(
        F.col("shipments.id"),
        F.col("shipments.cost"),
        F.col("orders.amount"),
        F.col("orders.discount"),
        F.col("orders.status"),
    )
)
