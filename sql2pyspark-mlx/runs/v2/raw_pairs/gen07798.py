from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .filter(F.col("delivered_date") > "2024-01-01")
    .groupBy("order_id")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.max("cost").alias("max_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "left")
    .filter(F.col("sbo.avg_cost") >= 55)
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.store_id"),
        F.col("sbo.avg_cost"),
        F.col("sbo.max_cost"),
    )
    .orderBy("avg_cost", F.col("discount").desc())
)
