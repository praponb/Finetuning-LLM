from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("shipments.order_id"), "inner")
    .filter(F.col("orders.amount").between(1130, 1400) & (F.col("orders.status") != "PENDING"))
    .select(
        F.col("shipments.id"),
        F.col("shipments.cost"),
        F.col("shipments.order_id"),
        F.col("shipments.delivered_date"),
        F.col("orders.customer_id"),
    )
    .orderBy("id")
    .limit(10)
)
