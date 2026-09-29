from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "left")
    .filter(F.col("orders.shipping_fee") <= 17)
    .groupBy(F.col("stores.city"))
    .agg(
        F.min(F.col("shipments.cost")).alias("min_cost"),
        F.countDistinct(F.col("shipments.cost")).alias("unique_statuss"),
    )
    .orderBy(F.col("city").desc())
    .limit(10)
)
