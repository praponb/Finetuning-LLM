from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.filter(F.col("order_id").isin([10, 15, 29, 37])).alias("s"), F.col("o.id") == F.col("s.order_id"), "left_semi")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.store_id"),
        F.col("o.shipping_fee"),
    )
)
