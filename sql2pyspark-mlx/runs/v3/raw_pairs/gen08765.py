from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.countDistinct("cost").alias("unique_carriers"),
        F.sum(F.when(F.col("status") == "LOST", 1).otherwise(0)).alias("lost_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .select(
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("ot.unique_carriers"),
        F.col("ot.lost_count"),
    )
    .orderBy(F.col("unique_carriers").desc())
)
