from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .select(
        F.col("shipments.id"),
        F.col("shipments.delivered_date"),
        F.col("orders.discount"),
        F.col("stores.sqft"),
    )
    .orderBy(F.col("id").desc(), F.col("delivered_date").desc())
)
