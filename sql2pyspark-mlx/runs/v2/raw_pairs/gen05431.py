from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("s.order_id").isNotNull())
    .select(
        F.col("o.channel"),
        F.col("o.order_date"),
        F.col("o.store_id"),
        F.col("s.order_id"),
        F.col("s.status"),
        (F.col("o.amount") + F.col("o.shipping_fee")).alias("amount_with_shipping"),
    )
)
