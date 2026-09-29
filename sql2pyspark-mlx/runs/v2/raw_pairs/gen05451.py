from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .filter(F.col("shipments.delivered_date") < "2023-10-01")
    .select(
        F.col("shipments.id"),
        F.col("shipments.cost"),
        F.col("orders.order_date"),
        F.col("stores.region"),
    )
    .orderBy(F.col("cost").desc())
)
