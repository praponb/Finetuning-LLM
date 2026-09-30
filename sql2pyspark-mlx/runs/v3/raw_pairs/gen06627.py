from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .filter(F.col("shipped_date") > "2023-08-01")
    .groupBy("order_id")
    .agg(
        F.countDistinct("cost").alias("unique_carriers"),
        F.countDistinct("cost").alias("unique_statuss"),
    )
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .filter(F.col("t.unique_statuss") > 5)
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("t.unique_carriers"),
        F.col("t.unique_statuss"),
    )
    .orderBy("unique_carriers", "id")
    .limit(10)
)
