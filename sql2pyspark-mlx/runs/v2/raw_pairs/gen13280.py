from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .groupBy(F.quarter(F.col("orders.order_date")).alias("order_quarter"))
    .agg(F.sum(F.col("order_items.unit_price")).alias("total_unit_price"))
    .orderBy("order_quarter")
    .limit(50)
)
