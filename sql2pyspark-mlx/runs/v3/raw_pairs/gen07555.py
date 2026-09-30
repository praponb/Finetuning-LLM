from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .filter(F.col("carrier").isin(["DHL", "Kerry", "UPS"]))
    .groupBy("order_id")
    .agg(
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.count(F.when(F.col("carrier") == "FedEx", 1)).alias("fedex_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.status"),
        F.col("t.carrier_list"),
        F.col("t.fedex_count"),
    )
    .orderBy("status", "id")
)
