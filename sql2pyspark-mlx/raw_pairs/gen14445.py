from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(F.col("o.store_id").isin([11, 19]))
    .select(F.col("s.cost"), F.col("o.customer_id"))
    .orderBy("customer_id")
)
