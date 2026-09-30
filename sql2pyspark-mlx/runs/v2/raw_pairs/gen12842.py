from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("s.status").isin(["RETURNED", "DELIVERED", "IN_TRANSIT"]))
    .select(
        F.col("s.id"),
        F.col("s.carrier"),
        F.col("s.order_id"),
        F.col("s.delivered_date"),
        F.col("o.order_date"),
        F.col("o.shipping_fee"),
        F.col("o.customer_id"),
    )
    .orderBy("delivered_date")
)
