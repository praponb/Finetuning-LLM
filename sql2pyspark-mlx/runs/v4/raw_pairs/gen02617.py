from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("orders.store_id").isin([7, 31, 37, 38]))
    .select(
        F.col("shipments.shipped_date"),
        F.col("shipments.carrier"),
        F.col("orders.amount"),
        F.col("orders.customer_id"),
        F.col("orders.order_date"),
    )
)
