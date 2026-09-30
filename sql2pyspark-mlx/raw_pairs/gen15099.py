from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("carrier") == "Kerry", 1).otherwise(0)).alias("kerry_count"),
        F.min("cost").alias("min_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.status"),
        F.col("sbo.kerry_count"),
        F.col("sbo.min_cost"),
    )
    .orderBy(F.col("kerry_count").desc(), F.col("shipping_fee").desc())
)
