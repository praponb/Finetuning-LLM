from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("shipments.order_id"), "inner")
    .groupBy(F.col("orders.channel"), F.col("orders.status"))
    .agg(
        F.count(F.when(F.col("shipments.status") == "LOST", 1)).alias("lost_count"),
        F.round(((100 * F.sum(F.when(F.col("shipments.carrier") == "UPS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_ups"),
    )
    .filter(F.col("pct_ups") >= 43)
    .orderBy("status")
)
