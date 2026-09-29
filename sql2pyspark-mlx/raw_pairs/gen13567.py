from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(F.col("o.customer_id").isin([7, 20, 26, 33]))
    .select(F.col("s.carrier"), F.col("o.store_id"))
)
