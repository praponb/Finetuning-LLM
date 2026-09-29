from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter((F.col("s.carrier") == "Kerry") & F.col("o.store_id").isin([8, 25, 35]))
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("s.cost"),
        F.col("s.order_id"),
        F.col("o.shipping_fee"),
        F.col("o.channel"),
        (F.col("o.discount") * F.col("o.shipping_fee")).alias("discount_times_shipping_fee"),
    )
)
