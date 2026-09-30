from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.status"),
        F.col("o.discount"),
        F.col("o.customer_id"),
    )
)
