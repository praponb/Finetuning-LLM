from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("status") == "IN_TRANSIT", F.col("cost")).otherwise(0)).alias("in_transit_cost"),
        F.count("*").alias("shipment_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "left")
    .filter(F.col("ot.in_transit_cost") > 65)
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("ot.in_transit_cost"),
        F.col("ot.shipment_count"),
    )
    .orderBy(F.col("in_transit_cost").desc())
)
