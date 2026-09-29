from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("shipments.order_id"), "left")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("orders.store_id") == 2)
    .groupBy(F.col("orders.status"))
    .agg(F.countDistinct(F.col("shipments.cost")).alias("unique_statuss"))
    .orderBy("status")
)
