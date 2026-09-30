from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_shipments.alias("shipments"), F.col("shipments.order_id") == F.col("orders.id"), "left")
    .filter(~F.col("shipments.carrier").isin(["DHL", "Flash", "UPS"]))
    .groupBy(F.col("orders.channel"))
    .agg(
        F.count(F.col("shipments.id")).alias("shipment_count"),
        F.round(((100 * F.sum(F.when(F.col("shipments.carrier") == "Flash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_flash"),
    )
)
