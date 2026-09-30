from pyspark.sql import functions as F

excluded_orders = df_orders.filter(F.col("status") != "COMPLETED").select("id")

result = (
    df_shipments.alias("shipments")
    .join(excluded_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "left_anti")
    .filter(F.col("shipments.order_id").isNotNull() | F.lit(excluded_orders.isEmpty()))
    .select(
        F.col("shipments.id"),
        F.col("shipments.status"),
        F.col("shipments.delivered_date"),
        F.col("shipments.carrier"),
    )
)
