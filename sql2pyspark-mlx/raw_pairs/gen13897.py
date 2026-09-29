from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.filter(F.col("cost").between(48, 93)).alias("s"), F.col("o.id") == F.col("s.order_id"), "left_anti")
    .select(F.col("o.id"), F.col("o.store_id"))
)
