from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .filter(F.col("o.customer_id") == 32)
    .select(F.col("s.id"), F.col("s.status"), F.col("o.store_id"))
    .orderBy("store_id")
)
