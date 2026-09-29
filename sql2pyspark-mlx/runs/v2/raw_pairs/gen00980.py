from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("shipments.order_id"), "inner")
    .select(
        F.col("shipments.id"),
        F.col("shipments.cost"),
        F.col("shipments.order_id"),
        F.col("shipments.status"),
        F.col("orders.store_id"),
        F.col("orders.discount"),
        F.col("orders.order_date"),
        F.ceil(F.col("orders.amount")).alias("amount_whole"),
    )
    .orderBy(F.col("id").desc())
)
