from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.status").isin(["RETURNED", "PENDING", "CANCELLED"]))
    .select(
        F.col("s.id"),
        F.col("s.order_id"),
        F.col("o.amount"),
        F.col("o.shipping_fee"),
    )
    .orderBy("id")
)
