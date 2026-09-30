from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .select(
        F.col("s.id"),
        F.col("s.order_id"),
        F.col("o.channel"),
        F.col("o.discount"),
        F.col("o.customer_id"),
    )
)
