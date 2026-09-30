from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.customer_id").isNull())
    .groupBy(F.col("o.id"), F.col("o.status"))
    .agg(
        F.count(F.col("s.id")).alias("shipment_count"),
        F.round(((100 * F.sum(F.when(F.col("s.carrier") == "UPS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_ups"),
        F.sort_array(F.collect_set(F.col("s.carrier"))).alias("carrier_list"),
    )
    .orderBy(F.col("status").desc(), "id")
    .limit(50)
)
