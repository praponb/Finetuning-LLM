from pyspark.sql import functions as F

result = (
    df_shipments.alias("shipments")
    .join(df_orders.alias("orders"), F.col("shipments.order_id") == F.col("orders.id"), "inner")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "outer")
    .filter(F.col("stores.opened_date") >= "2023-04-01")
    .groupBy(
        F.month(F.col("stores.opened_date")).alias("opened_month"),
        F.col("stores.city"),
    )
    .agg(F.count("*").alias("total_count"))
)
