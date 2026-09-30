from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .select(
        F.col("s.carrier"),
        F.col("s.delivered_date"),
        F.col("s.status"),
        F.col("o.amount"),
        F.col("o.order_date"),
        F.col("o.customer_id"),
    )
)
