from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("shipments.order_id"), "left")
    .filter(F.col("shipments.carrier").isin(["DHL", "Kerry", "UPS"]))
    .select(
        F.col("shipments.carrier"),
        F.col("shipments.delivered_date"),
        F.col("shipments.status"),
        F.col("orders.channel"),
        F.col("orders.amount"),
        F.col("orders.shipping_fee"),
        F.coalesce(F.col("shipments.carrier"), F.lit("Unknown")).alias("carrier_filled"),
    )
)
