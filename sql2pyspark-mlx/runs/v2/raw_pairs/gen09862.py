from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .filter(F.col("s.order_id").isNotNull())
    .join(df_orders.filter(F.col("store_id").isin([25, 35, 40])).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_anti")
    .select(F.col("s.id"), F.col("s.carrier"))
)
