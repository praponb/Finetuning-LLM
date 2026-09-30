from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("s.status").isin(["LOST", "RETURNED"]))
    .select(
        F.col("s.shipped_date"),
        F.col("s.status"),
        F.col("s.order_id"),
        F.col("o.status").alias("order_status"),
        F.col("o.order_date"),
        F.col("o.channel"),
        F.ceil(F.col("o.shipping_fee")).alias("shipping_fee_whole"),
    )
    .orderBy(F.col("order_id").desc(), "status")
)
