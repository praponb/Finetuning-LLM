from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left_anti")
    .filter(F.col("o.customer_id") == 8)
    .select(F.col("o.id"), F.col("o.order_date"))
)
