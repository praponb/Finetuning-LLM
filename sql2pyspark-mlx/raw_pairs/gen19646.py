from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left")
    .filter(F.col("o.customer_id").isNull() | F.col("o.discount").between(0.25, 0.31))
    .select(F.col("o.store_id"), F.col("s.delivered_date"))
)
