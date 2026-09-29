from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("s.order_id").isin([19, 32]))
    .select(F.col("s.order_id"), F.col("o.amount"), F.col("o.status"))
)
