from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.filter(F.col("shipped_date").between("2023-07-03", "2024-05-01")).alias("shipments"), F.col("orders.id") == F.col("shipments.order_id"), "left_semi")
    .filter(F.col("orders.customer_id").isin([6, 10, 12, 34]))
    .select(
        F.col("orders.id"),
        F.col("orders.discount"),
        F.col("orders.shipping_fee"),
        F.col("orders.amount"),
    )
)
