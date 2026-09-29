from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_orders.alias("orders"), F.col("stores.id") == F.col("orders.store_id"), "inner")
    .filter(F.col("orders.order_date").between("2024-06-01", "2025-01-06"))
    .groupBy(F.col("stores.id"))
    .agg(
        F.count(F.col("orders.id")).alias("order_count"),
        F.max(F.col("orders.order_date")).alias("latest_order_date"),
    )
    .orderBy(F.col("id").desc())
)
