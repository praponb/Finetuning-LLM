from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("s.order_id") == F.col("o.id"), "inner")
    .select(
        F.col("o.customer_id"),
        F.col("o.store_id"),
        F.col("o.status"),
        F.col("s.status").alias("shipment_status"),
        F.col("s.shipped_date"),
        F.col("s.cost"),
        F.round((F.col("o.shipping_fee") * 100), 2).alias("shipping_fee_scaled"),
    )
    .orderBy("cost")
)
