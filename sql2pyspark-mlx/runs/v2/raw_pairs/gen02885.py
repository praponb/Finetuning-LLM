from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left_anti")
    .filter(F.col("o.status") == "PENDING")
    .select(F.col("o.id"), F.col("o.store_id"), F.col("o.amount"))
    .orderBy(F.col("store_id").desc(), "id")
    .limit(1)
)
