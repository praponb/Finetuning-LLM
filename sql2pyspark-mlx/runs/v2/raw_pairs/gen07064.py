from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.filter(F.col("store_id").isin([4, 12, 27, 39])).alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "left_semi")
    .select(F.col("shipments.id"), F.col("shipments.carrier"))
)
