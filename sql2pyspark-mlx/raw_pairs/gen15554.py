from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select(
        F.col("s.order_id"),
        F.col("s.status"),
        F.col("s.shipped_date"),
        F.col("o.order_date"),
        F.round((F.col("o.shipping_fee") * 1.15), 2).alias("shipping_fee_scaled"),
    )
)
