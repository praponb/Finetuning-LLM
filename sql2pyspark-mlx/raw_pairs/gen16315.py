from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("shipments.delivered_date").between("2023-07-15", "2024-05-01"))
    .groupBy(F.col("orders.customer_id"))
    .agg(
        F.max(F.col("shipments.delivered_date")).alias("last_delivered_date"),
        F.avg(F.col("shipments.cost")).alias("avg_cost"),
    )
    .orderBy(F.col("last_delivered_date").desc())
)
