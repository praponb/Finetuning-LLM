from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("s.carrier").isin(["FedEx", "Flash", "UPS"]) & (F.col("s.order_id") == 22))
    .select(
        F.col("s.status"),
        F.col("s.delivered_date"),
        F.col("o.shipping_fee"),
        F.col("o.customer_id"),
        F.col("o.store_id"),
    )
)
