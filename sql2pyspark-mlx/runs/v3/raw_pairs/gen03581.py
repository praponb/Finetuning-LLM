from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "outer")
    .select(
        F.col("shipments.cost"),
        F.col("orders.order_date"),
        F.col("orders.store_id"),
    )
)
