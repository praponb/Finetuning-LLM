from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left")
    .filter(F.col("o.status").isin(["CANCELLED", "PENDING", "SHIPPED"]))
    .select(
        F.col("o.customer_id"),
        F.col("s.status"),
        F.col("s.order_id"),
        F.col("s.cost"),
    )
)
