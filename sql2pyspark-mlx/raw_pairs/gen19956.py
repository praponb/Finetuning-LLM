from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .filter((F.col("o.amount") > 480) & (F.col("s.order_id") == 3))
    .select(
        F.col("s.delivered_date"),
        F.col("s.status"),
        F.col("o.customer_id"),
        F.col("o.store_id"),
        F.col("o.status").alias("order_status"),
    )
)
