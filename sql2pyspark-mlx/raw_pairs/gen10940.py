from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .filter(F.col("status") == "DELIVERED")
    .groupBy("order_id")
    .agg(
        F.min("delivered_date").alias("min_delivered_date"),
        F.round(((100 * F.sum(F.when(F.col("status") == "RETURNED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_returned"),
    )
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "inner")
    .select(
        F.col("o.amount"),
        F.col("sbo.min_delivered_date"),
        F.col("sbo.pct_returned"),
    )
    .orderBy(F.col("amount").desc())
)
