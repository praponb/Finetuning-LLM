from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.filter(F.col("discount") <= 0.38).alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "left_semi")
    .filter(F.col("shipments.order_id").isin([1, 15, 31, 33]))
    .select(
        F.col("shipments.id"),
        F.col("shipments.cost"),
        F.col("shipments.order_id"),
        F.col("shipments.status"),
    )
    .orderBy(F.col("order_id").desc())
)
